use std::{
    future::Future,
    io,
    marker::PhantomData,
    pin::Pin,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    task::{Context, Poll},
    time::Duration,
};

use chrono::{DateTime, Utc};
use futures::{
    channel::{mpsc, oneshot},
    future::{select, Either},
    pin_mut, FutureExt, SinkExt, Stream, StreamExt,
};

use crate::{
    connection::Connection,
    consumer::{
        config::ConsumerConfig,
        data::{DeadLetterPolicy, EngineMessage, MessageData, MessageIdDataReceiver},
        engine::ConsumerEngine,
        message::Message,
    },
    error::{ConnectionError, ConsumerError},
    message::proto::{MessageIdData, Schema},
    proto::CommandConsumerStatsResponse,
    retry_op::retry_subscribe_consumer,
    BrokerAddress, DeserializeMessage, Error, Executor, Payload, Pulsar,
};

// this is entirely public for use in reader.rs
pub struct TopicConsumer<T: DeserializeMessage, Exe: Executor> {
    pub(crate) consumer_id: u64,
    pub(crate) config: ConsumerConfig,
    topic: String,
    messages: Pin<Box<MessageIdDataReceiver>>,
    engine_tx: mpsc::UnboundedSender<EngineMessage<Exe>>,
    /// Times the answers this consumer awaits from its engine.
    executor: Arc<Exe>,
    /// How long this consumer waits for the answer to an acknowledgement or a negative
    /// acknowledgement, the client's operation timeout.
    operation_timeout: Duration,
    data_type: PhantomData<fn(Payload) -> T::Output>,
    pub(crate) dead_letter_policy: Option<DeadLetterPolicy>,
    pub(super) last_message_received: Option<DateTime<Utc>>,
    pub(super) messages_received: u64,
}

impl<T: DeserializeMessage, Exe: Executor> TopicConsumer<T, Exe> {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub(super) async fn new(
        client: Pulsar<Exe>,
        topic: String,
        addr: BrokerAddress,
        config: ConsumerConfig,
    ) -> Result<TopicConsumer<T, Exe>, Error> {
        static CONSUMER_ID_GENERATOR: AtomicU64 = AtomicU64::new(0);

        let ConsumerConfig {
            subscription,
            sub_type,
            batch_size,
            consumer_name,
            consumer_id,
            unacked_message_redelivery_delay,
            options,
            dead_letter_policy,
        } = config.clone();
        let consumer_id =
            consumer_id.unwrap_or_else(|| CONSUMER_ID_GENERATOR.fetch_add(1, Ordering::SeqCst));
        let batch_size = batch_size.unwrap_or(1000);
        let mut connection = client.manager.get_connection(&addr).await?;

        let messages = retry_subscribe_consumer(
            &client,
            &mut connection,
            addr,
            &topic,
            &subscription,
            sub_type,
            consumer_id,
            &consumer_name,
            &options,
            batch_size,
        )
        .await?;

        let (engine_tx, engine_rx) = mpsc::unbounded();

        if unacked_message_redelivery_delay.is_some() {
            let mut redelivery_tx = engine_tx.clone();
            let mut interval = client.executor.interval(Duration::from_millis(500));
            let res = client.executor.spawn(Box::pin(async move {
                while interval.next().await.is_some() {
                    if redelivery_tx
                        .send(EngineMessage::UnackedRedelivery)
                        .await
                        .is_err()
                    {
                        // Consumer shut down - stop ticker
                        break;
                    }
                }
            }));
            if res.is_err() {
                return Err(Error::Executor);
            }
        }
        let receiver_queue_size = options.receiver_queue_size.unwrap_or(1000);
        let (tx, rx) = mpsc::channel(receiver_queue_size as usize);
        let mut c = ConsumerEngine::new(
            client.clone(),
            connection.clone(),
            topic.clone(),
            subscription.clone(),
            sub_type,
            consumer_id,
            consumer_name,
            tx,
            messages,
            engine_rx,
            batch_size,
            unacked_message_redelivery_delay,
            dead_letter_policy.clone(),
            options.clone(),
        );
        let engine_task = client.executor.spawn(Box::pin(async move {
            c.engine()
                .map(|res| {
                    debug!("consumer engine stopped: {:?}", res);
                })
                .await;
        }));
        if engine_task.is_err() {
            return Err(Error::Executor);
        }

        Ok(TopicConsumer {
            consumer_id,
            config,
            topic,
            messages: Box::pin(rx),
            engine_tx,
            executor: client.executor.clone(),
            operation_timeout: client.operation_retry_options.operation_timeout,
            data_type: PhantomData,
            dead_letter_policy,
            last_message_received: None,
            messages_received: 0,
        })
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn topic(&self) -> String {
        self.topic.clone()
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn connection(&mut self) -> Result<Arc<Connection<Exe>>, Error> {
        let (resolver, response) = oneshot::channel();
        self.engine_tx
            .send(EngineMessage::GetConnection(resolver))
            .await
            .map_err(|_| ConsumerError::Connection(ConnectionError::Disconnected))?;

        response.await.map_err(|oneshot::Canceled| {
            error!("the consumer engine dropped the request");
            ConnectionError::Disconnected.into()
        })
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn get_stats(&mut self) -> Result<CommandConsumerStatsResponse, Error> {
        let consumer_id = self.consumer_id;
        let conn = self.connection().await?;
        let consumer_stats_response = conn.sender().get_consumer_stats(consumer_id).await?;
        Ok(consumer_stats_response)
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn check_connection(&mut self) -> Result<(), Error> {
        let conn = self.connection().await?;
        info!("check connection for id {}", conn.id());
        conn.sender().send_ping().await?;
        Ok(())
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn ack(&mut self, msg: &Message<T>) -> Result<(), ConsumerError> {
        self.acknowledge(vec![msg.message_id().clone()], false)
            .await
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn ack_with_id(&mut self, msg_id: MessageIdData) -> Result<(), ConsumerError> {
        self.acknowledge(vec![msg_id], false).await
    }

    /// acknowledges the messages with the given IDs, in one command
    ///
    /// with `ConsumerOptions::ack_receipt_enabled`, one answer from the broker confirms them all
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn ack_with_ids(&mut self, msg_ids: Vec<MessageIdData>) -> Result<(), ConsumerError> {
        if msg_ids.is_empty() {
            return Ok(());
        }
        self.acknowledge(msg_ids, false).await
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub(crate) fn acker(&self) -> mpsc::UnboundedSender<EngineMessage<Exe>> {
        self.engine_tx.clone()
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn cumulative_ack(&mut self, msg: &Message<T>) -> Result<(), ConsumerError> {
        self.acknowledge(vec![msg.message_id().clone()], true).await
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn cumulative_ack_with_id(
        &mut self,
        msg_id: MessageIdData,
    ) -> Result<(), ConsumerError> {
        self.acknowledge(vec![msg_id], true).await
    }

    /// Acknowledges `msg_ids`. Without `ConsumerOptions::ack_receipt_enabled` this returns once
    /// the engine queued an acknowledgement for each of them; with it, once the broker answered
    /// the one command that carries them all, within the client's operation timeout.
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    async fn acknowledge(
        &mut self,
        msg_ids: Vec<MessageIdData>,
        cumulative: bool,
    ) -> Result<(), ConsumerError> {
        if !self.config.options.ack_receipt_enabled {
            for msg_id in msg_ids {
                self.engine_tx
                    .send(EngineMessage::Ack(msg_id, cumulative))
                    .await?;
            }
            return Ok(());
        }
        let (receipt_tx, receipt) = oneshot::channel();
        self.engine_tx
            .send(EngineMessage::AckWithReceipt(
                msg_ids, cumulative, receipt_tx,
            ))
            .await?;
        self.await_engine_answer(async move {
            let answer = receipt
                .await
                .map_err(|oneshot::Canceled| ConnectionError::Disconnected)??;
            answer.await
        })
        .await
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn nack(&mut self, msg: &Message<T>) -> Result<(), ConsumerError> {
        self.negatively_acknowledge(msg.message_id().clone()).await
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn nack_with_id(&mut self, msg_id: MessageIdData) -> Result<(), ConsumerError> {
        self.negatively_acknowledge(msg_id).await
    }

    /// Asks the broker to redeliver `msg_id`, and returns once the engine sent the redelivery
    /// request, within the client's operation timeout, or with the error that kept it from
    /// sending it. The broker does not answer a redelivery request.
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    async fn negatively_acknowledge(&mut self, msg_id: MessageIdData) -> Result<(), ConsumerError> {
        let (outcome_tx, outcome) = oneshot::channel();
        self.engine_tx
            .send(EngineMessage::Nack(msg_id, outcome_tx))
            .await?;
        self.await_engine_answer(async move {
            outcome
                .await
                .map_err(|oneshot::Canceled| ConnectionError::Disconnected)?
        })
        .await
    }

    /// Waits for an answer the engine gives, within the client's operation timeout. The engine
    /// takes its messages in order, so a reconnection it performs first holds the answer back.
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    async fn await_engine_answer(
        &self,
        answer: impl Future<Output = Result<(), ConnectionError>>,
    ) -> Result<(), ConsumerError> {
        let deadline = self.executor.delay(self.operation_timeout);
        pin_mut!(answer);
        pin_mut!(deadline);
        match select(answer, deadline).await {
            Either::Left((answered, _)) => answered.map_err(ConsumerError::Connection),
            Either::Right(_) => Err(ConsumerError::Connection(ConnectionError::Io(
                io::Error::new(
                    io::ErrorKind::TimedOut,
                    format!(
                        "consumer {} got no answer from its engine within {:?}",
                        self.consumer_id, self.operation_timeout
                    ),
                ),
            ))),
        }
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn seek(
        &mut self,
        message_id: Option<MessageIdData>,
        timestamp: Option<u64>,
    ) -> Result<(), Error> {
        let consumer_id = self.consumer_id;
        self.connection()
            .await?
            .sender()
            .seek(consumer_id, message_id, timestamp)
            .await?;
        Ok(())
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn unsubscribe(&mut self) -> Result<(), Error> {
        let consumer_id = self.consumer_id;
        self.connection()
            .await?
            .sender()
            .unsubscribe(consumer_id)
            .await?;
        Ok(())
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn close(&mut self) -> Result<(), Error> {
        let consumer_id = self.consumer_id;
        self.connection()
            .await?
            .sender()
            .close_consumer(consumer_id)
            .await?;
        Ok(())
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn get_last_message_id(&mut self) -> Result<MessageIdData, Error> {
        let consumer_id = self.consumer_id;
        let conn = self.connection().await?;
        let get_last_message_id_response = conn.sender().get_last_message_id(consumer_id).await?;
        Ok(get_last_message_id_response.last_message_id)
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn last_message_received(&self) -> Option<DateTime<Utc>> {
        self.last_message_received
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn messages_received(&self) -> u64 {
        self.messages_received
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn config(&self) -> &ConsumerConfig {
        &self.config
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn create_message(&self, message_id: MessageIdData, payload: Payload) -> Message<T> {
        let message_id = MessageData {
            id: message_id,
            batch_size: payload.metadata.num_messages_in_batch,
        };
        Message::new(&self.topic, message_id, payload)
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub(crate) async fn get_schema(
        &mut self,
        version: Option<Vec<u8>>,
    ) -> Result<Option<Schema>, Error> {
        let conn = self.connection().await?;
        let schema_response = conn.sender().get_schema(&self.topic, version).await?;
        Ok(schema_response.schema)
    }
}

impl<T: DeserializeMessage, Exe: Executor> Stream for TopicConsumer<T, Exe> {
    type Item = Result<Message<T>, Error>;

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.messages.as_mut().poll_next(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Ready(Some(Ok((id, payload)))) => {
                self.last_message_received = Some(Utc::now());
                self.messages_received += 1;
                Poll::Ready(Some(Ok(self.create_message(id, payload))))
            }
            Poll::Ready(Some(Err(e))) => {
                error!("we are using in the single-consumer and we got an error, {e}");
                Poll::Ready(Some(Err(e)))
            }
        }
    }
}
