import json
import logging
from typing import Optional

import confluent_kafka as ck

from kafka_excel_sim.message import MessageMetadata
from kafka_excel_sim.config import KafkaConfig

logger: logging.Logger = logging.getLogger(__name__)


class KafkaConnectionError(Exception):
    """Raised when the producer cannot be initialised."""
    pass

class KafkaClient:
    """Manages the Confluent Kafka producer lifecycle.

    Handles connection setup (including optional SASL authentication),
    message delivery, and graceful shutdown.

    Attributes:
        broker_endpoint:  Bootstrap servers string.
        producer_config:  Dict of extra producer options (e.g. credentials).
    """

    def __init__(self, producer_config: KafkaConfig) -> None:
        self.producer_config: KafkaConfig = producer_config
        self._closed: bool = False

        config: dict = {
            'bootstrap.servers': self.producer_config.servers,
            'sasl.mechanism': self.producer_config.sasl_mechanism,
            'security.protocol': self.producer_config.security_protocol,
            'sasl.username': self.producer_config.username,
            'sasl.password': self.producer_config.password,
        }

        try:
            self.producer = ck.Producer(config)
        except Exception as exc:
            raise KafkaConnectionError(f'Failed to create producer: {exc}') from exc

    def send_message(
        self, topic: str, key: Optional[str], message: dict
    ) -> MessageMetadata:
        """Serialise and send a message, then block until delivery is confirmed.

        Args:
            topic:   Target topic name.
            key:     Optional partition key string.
            message: Payload dict — will be serialised to JSON bytes.

        Returns:
            MessageMetadata containing topic, partition, and offset.

        Raises:
            RuntimeError: If the producer has already been closed.
            Exception:    If delivery fails.
        """
        if self._closed:
            raise RuntimeError("Producer is closed")

        value_bytes = json.dumps(message).encode("utf-8")
        key_bytes = key.encode("utf-8") if key else None

        delivery_result: dict = {
            "success": False,
            "partition": -1,
            "offset": -1,
            "error": None,
        }

        def _on_delivery(err, msg) -> None:
            if err:
                delivery_result["error"] = err
            else:
                delivery_result["success"] = True
                delivery_result["partition"] = msg.partition()
                delivery_result["offset"] = msg.offset()

        self.producer.produce(
            topic=topic,
            key=key_bytes,
            value=value_bytes,
            callback=_on_delivery,
        )
        self.producer.flush()

        if not delivery_result["success"]:
            error_msg = (
                str(delivery_result["error"])
                if delivery_result["error"]
                else "Unknown error"
            )
            raise Exception(f"Failed to send message: {error_msg}")
        else:
            logger.debug(f'Sent message in topic {topic}, partition {delivery_result["partition"]}, \
                offset {delivery_result["offset"]}')

        return MessageMetadata(
            topic=topic,
            partition=delivery_result["partition"],
            offset=delivery_result["offset"],
        )

    def close(self) -> None:
        """Flush any pending messages and mark the producer as closed."""
        if not self._closed:
            self.producer.flush()
            self._closed = True
