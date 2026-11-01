use std::{future::Future, pin::Pin, sync::Arc};

use futures::channel::{mpsc, oneshot};

use crate::{
    connection::Connection,
    error::ConnectionError,
    message::{proto::MessageIdData, Message as RawMessage},
    Error, Executor, Payload,
};

pub type MessageIdDataReceiver = mpsc::Receiver<Result<ReceivedMessage, Error>>;

/// One delivery from the consumer engine, including metadata from its broker command.
pub struct ReceivedMessage {
    pub message_id: MessageIdData,
    pub payload: Payload,
    pub redelivery_count: Option<u32>,
}

/// Resolves once the broker answered one acknowledgement, with the server error the answer
/// carries if the broker refused it. It is bounded by the connection's operation timeout.
pub type AckReceipt = Pin<Box<dyn Future<Output = Result<(), ConnectionError>> + Send>>;

pub enum EngineEvent<Exe: Executor> {
    Message(Option<RawMessage>),
    EngineMessage(Option<EngineMessage<Exe>>),
}

pub enum EngineMessage<Exe: Executor> {
    /// An acknowledgement that returns once it is queued for the connection.
    Ack(MessageIdData, bool),
    /// An acknowledgement of one or more messages whose answer the caller awaits. The engine sends
    /// them in one command with a request id and hands back the receipt that resolves with the
    /// broker's answer, or the error that kept it from being sent, so the engine never waits for
    /// the broker itself.
    AckWithReceipt(
        Vec<MessageIdData>,
        bool,
        oneshot::Sender<Result<AckReceipt, ConnectionError>>,
    ),
    /// A negative acknowledgement, answered with whether its redelivery request was sent. The
    /// broker does not answer a redelivery request, so a sent one is all the client can confirm.
    Nack(MessageIdData, oneshot::Sender<Result<(), ConnectionError>>),
    UnackedRedelivery,
    GetConnection(oneshot::Sender<Arc<Connection<Exe>>>),
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct MessageData {
    pub id: MessageIdData,
    pub batch_size: Option<i32>,
}

#[derive(Debug, Clone)]
pub struct DeadLetterPolicy {
    /// Maximum number of times that a message will be redelivered before being sent to the dead
    /// letter queue.
    pub max_redeliver_count: usize,
    /// Name of the dead topic where the failing messages will be sent.
    pub dead_letter_topic: String,
}
