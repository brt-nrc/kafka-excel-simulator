import logging
from kafka_excel_sim.message import Message, MessageMetadata
from kafka_excel_sim.kafka_client import KafkaClient

logger = logging.getLogger(__name__)


class MessageSender:
    """Sends a Message via the KafkaClient.

    Owns the serialisation contract: it takes a Message (whose payload is
    already a plain dict) and delegates the JSON encoding + delivery to
    KafkaClient.

    Attributes:
        client: The KafkaClient instance used for delivery.
    """

    def __init__(self, client: KafkaClient) -> None:
        self.client = client

    def send(self, message: Message) -> MessageMetadata:
        """Send a single message and return its delivery metadata.

        Args:
            message: A fully constructed Message object.

        Returns:
            MessageMetadata with topic, partition, and offset from the broker.
        """
        try:
            response: MessageMetadata = self.client.send_message(
                topic = message.topic,
                key = message.key,
                message = message.payload,
            )
        except Exception as exc:
            raise exc
        return response