use crate::{
    error::ConnectionError,
    message::{parse_batched_message, proto::MessageIdData, BatchedMessage, Metadata},
    Payload,
};

pub struct BatchedMessageIterator {
    messages: std::vec::IntoIter<BatchedMessage>,
    message_id: MessageIdData,
    metadata: Metadata,
    total_messages: u32,
    current_index: u32,
}

impl BatchedMessageIterator {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn new(message_id: MessageIdData, payload: Payload) -> Result<Self, ConnectionError> {
        let total_messages = payload
            .metadata
            .num_messages_in_batch
            .expect("expected batched message") as u32;
        let messages = parse_batched_message(total_messages, &payload.data)?;

        Ok(Self {
            messages: messages.into_iter(),
            message_id,
            total_messages,
            metadata: payload.metadata,
            current_index: 0,
        })
    }
}

impl Iterator for BatchedMessageIterator {
    type Item = (MessageIdData, Payload);

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn next(&mut self) -> Option<Self::Item> {
        let remaining = self.total_messages - self.current_index;
        if remaining == 0 {
            return None;
        }
        let index = self.current_index;
        self.current_index += 1;
        if let Some(batched_message) = self.messages.next() {
            let id = MessageIdData {
                batch_index: Some(index as i32),
                ..self.message_id.clone()
            };

            let metadata = Metadata {
                properties: batched_message.metadata.properties,
                partition_key: batched_message.metadata.partition_key,
                partition_key_b64_encoded: batched_message.metadata.partition_key_b64_encoded,
                ordering_key: batched_message.metadata.ordering_key,
                event_time: batched_message
                    .metadata
                    .event_time
                    .or(self.metadata.event_time),
                sequence_id: batched_message
                    .metadata
                    .sequence_id
                    .unwrap_or(self.metadata.sequence_id),
                ..self.metadata.clone()
            };

            let payload = Payload {
                metadata,
                data: batched_message.payload,
            };

            Some((id, payload))
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::proto::SingleMessageMetadata;

    #[test]
    fn batch_event_time_keeps_presence_and_member_precedence() {
        let member_times = [None, Some(0), Some(42), Some(u64::MAX)];
        let cases = [
            (None, [None, Some(0), Some(42), Some(u64::MAX)]),
            (Some(0), [Some(0), Some(0), Some(42), Some(u64::MAX)]),
            (Some(123), [Some(123), Some(0), Some(42), Some(u64::MAX)]),
            (
                Some(u64::MAX),
                [Some(u64::MAX), Some(0), Some(42), Some(u64::MAX)],
            ),
        ];
        for (envelope_time, expected_times) in cases {
            let mut data = Vec::new();
            for (index, event_time) in member_times.into_iter().enumerate() {
                BatchedMessage {
                    metadata: SingleMessageMetadata {
                        payload_size: 1,
                        event_time,
                        ..Default::default()
                    },
                    payload: vec![u8::try_from(index).unwrap()],
                }
                .serialize(&mut data);
            }
            let envelope = Metadata {
                producer_name: "event-producer".into(),
                sequence_id: 99,
                publish_time: 1234,
                event_time: envelope_time,
                num_messages_in_batch: Some(4),
                ..Default::default()
            };
            let position = MessageIdData {
                ledger_id: 1,
                entry_id: 2,
                partition: Some(3),
                ..Default::default()
            };
            let expanded: Vec<_> = BatchedMessageIterator::new(
                position.clone(),
                Payload {
                    metadata: envelope.clone(),
                    data,
                },
            )
            .unwrap()
            .collect();
            assert_eq!(expanded.len(), expected_times.len());
            for (index, ((id, payload), event_time)) in
                expanded.into_iter().zip(expected_times).enumerate()
            {
                assert_eq!(
                    id,
                    MessageIdData {
                        batch_index: Some(i32::try_from(index).unwrap()),
                        ..position.clone()
                    }
                );
                assert_eq!(payload.data, [u8::try_from(index).unwrap()]);
                assert_eq!(
                    payload.metadata,
                    Metadata {
                        event_time,
                        ..envelope.clone()
                    }
                );
            }
        }
    }

    #[test]
    fn each_batch_member_keeps_its_sequence_id_and_envelope_metadata() {
        let sequences = [Some(0), Some(42), None, Some(u64::MAX)];
        let mut data = Vec::new();
        for (index, sequence_id) in sequences.into_iter().enumerate() {
            BatchedMessage {
                metadata: SingleMessageMetadata {
                    payload_size: 1,
                    sequence_id,
                    ..Default::default()
                },
                payload: vec![u8::try_from(index).unwrap()],
            }
            .serialize(&mut data);
        }
        let envelope = Metadata {
            producer_name: "sequence-producer".into(),
            sequence_id: 99,
            publish_time: 1234,
            num_messages_in_batch: Some(4),
            ..Default::default()
        };
        let position = MessageIdData {
            ledger_id: 1,
            entry_id: 2,
            partition: Some(3),
            ..Default::default()
        };
        let expanded: Vec<_> = BatchedMessageIterator::new(
            position.clone(),
            Payload {
                metadata: envelope.clone(),
                data,
            },
        )
        .unwrap()
        .collect();
        assert_eq!(expanded.len(), sequences.len());
        for (index, ((id, payload), sequence_id)) in expanded.into_iter().zip(sequences).enumerate()
        {
            assert_eq!(
                id,
                MessageIdData {
                    batch_index: Some(i32::try_from(index).unwrap()),
                    ..position.clone()
                }
            );
            assert_eq!(payload.data, [u8::try_from(index).unwrap()]);
            assert_eq!(
                payload.metadata,
                Metadata {
                    sequence_id: sequence_id.unwrap_or(envelope.sequence_id),
                    ..envelope.clone()
                }
            );
        }
    }

    #[test]
    fn per_message_b64_flag_not_taken_from_batch_envelope() {
        let messages = [
            BatchedMessage {
                metadata: SingleMessageMetadata {
                    partition_key: Some("plain-key".into()),
                    partition_key_b64_encoded: Some(false),
                    payload_size: 1,
                    ..Default::default()
                },
                payload: b"a".to_vec(),
            },
            BatchedMessage {
                metadata: SingleMessageMetadata {
                    partition_key: Some("YmluYXJ5".into()),
                    partition_key_b64_encoded: Some(true),
                    payload_size: 1,
                    ..Default::default()
                },
                payload: b"b".to_vec(),
            },
        ];

        let mut data = Vec::new();
        for message in &messages {
            message.serialize(&mut data);
        }

        // Envelope flag deliberately disagrees with the first message. Before the
        // fix, `..self.metadata.clone()` leaked this onto every batch member.
        let payload = Payload {
            metadata: Metadata {
                producer_name: "test".into(),
                sequence_id: 0,
                publish_time: 0,
                num_messages_in_batch: Some(2),
                partition_key_b64_encoded: Some(true),
                ..Default::default()
            },
            data,
        };

        let expanded: Vec<_> = BatchedMessageIterator::new(
            MessageIdData {
                ledger_id: 1,
                entry_id: 2,
                ..Default::default()
            },
            payload,
        )
        .unwrap()
        .collect();

        assert_eq!(expanded.len(), 2);
        assert_eq!(
            expanded[0].1.metadata.partition_key.as_deref(),
            Some("plain-key")
        );
        assert_eq!(
            expanded[0].1.metadata.partition_key_b64_encoded,
            Some(false),
            "per-message flag must win over the batch envelope"
        );
        assert_eq!(expanded[1].1.metadata.partition_key_b64_encoded, Some(true));
    }
}
