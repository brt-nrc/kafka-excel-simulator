from dataclasses import dataclass, field


@dataclass
class MessageSchedule:
    """Represents a single entry in the simulation timetable.

    Each row from the 'timetable' Excel sheet becomes one instance.

    Attributes:
        offset_seconds: Number of seconds from simulation start when
                        this message should be sent.
        message_type:   The identifier that links to a payload template
                        in the 'types' sheet.
        overrides:      Dictionary of field name → value for any columns
                        the user marked as modifiable in the payload.
                        Keys match the placeholder names in the template.
        row_number:     The row number in the Excel sheet (1-indexed).
                        Used as a tiebreaker when multiple messages have
                        the same offset_seconds.
    """

    offset_seconds: float
    message_type: str
    overrides: dict = field(default_factory=dict)
    row_number: int = 0
