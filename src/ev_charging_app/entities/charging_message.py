from dataclasses import dataclass
from typing import Any, Literal

@dataclass(frozen=True)
class ChargingMessage:
    """
    Represents a charging event message.
    """
    event_type: Literal["start", "progress", "end"]
    session_id: str
    station_id: str
    ev_id: str
    payload: dict[str, Any]