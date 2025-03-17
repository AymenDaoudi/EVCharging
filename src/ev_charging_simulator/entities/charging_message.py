from dataclasses import dataclass
from typing import Any, Literal

@dataclass(frozen=True)
class ChargingMessage:
    """
    Represents a charging event message.
    """
    session_id: str
    session_number: int
    station_id: str
    ev_id: str
    # TODO: Add disconnection, error, etc.
    event_type: Literal["start", "completion"]
    payload: dict[str, Any]