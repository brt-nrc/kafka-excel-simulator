from dataclasses import dataclass


@dataclass
class KafkaConfig:
    """Holds Kafka broker connection settings.

    Attributes:
        broker:   Bootstrap servers string, e.g. "localhost:9092".
        username: SASL username. Empty string for unauthenticated brokers.
        password: SASL password. Empty string for unauthenticated brokers.
    """

    servers: str
    security_protocol: str = 'SASL_SSL'
    sasl_mechanism: str = 'PLAIN'
    username: str = ''
    password: str = ''

    @classmethod
    def from_dict(cls, data: dict[str, str]) -> "KafkaConfig":
        """Build a KafkaConfig from the 'kafka' section of load_config().

        Args:
            data: The inner dict for the 'kafka' category, e.g.
                  {"servers": "localhost:9092", "username": "", "password": ""}

        Raises:
            KeyError: If the required 'broker' key is missing.
        """
        return cls(
            servers             = data['servers'],
            username            = data.get('username', ''),
            password            = data.get('password', ''),
            security_protocol   = data.get('security_protocol', 'SASL_SSL'),
            sasl_mechanism      = data.get('sasl_mechanism', 'PLAIN'),
        )


@dataclass
class MessageConfig:
    keep_template_value_where_null: bool = True

    @classmethod
    def from_dict(cls, data: dict[str,str]) -> "MessageConfig":
        bool_map = {"true": True, "false": False}
        ktval: str = data.get('Keep template value where schedule override cell is null', "false")
        return cls(
            keep_template_value_where_null = bool_map.get(ktval.strip().lower(), True)
        )