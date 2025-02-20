from dataclasses import dataclass
from typing import Any, Literal

@dataclass(frozen=True)
class ChargingMessage:
    event_type: Literal["start", "progress", "end"]
    station_id: str
    ev_id: str
    payload: dict[str, Any] 