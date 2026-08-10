from dataclasses import dataclass
from typing import Optional


@dataclass
class Message:
    """A Kafka message ready to be sent.

    Attributes:
        topic:   Target Kafka topic.
        key:     Optional partition key (string).
        payload: The message body as a plain Python dict;
                 serialisation to JSON happens in MessageSender.
    """

    topic: str
    key: Optional[str]
    payload: dict


@dataclass
class MessageMetadata:
    """Delivery receipt returned by KafkaClient after a successful send.

    Attributes:
        topic:     Topic the message was written to.
        partition: Partition number assigned by the broker.
        offset:    Log offset assigned by the broker.
    """

    topic: str
    partition: int
    offset: int
