from datetime import datetime
import os
import random
import uuid

from repositories.kafka_repositories.kafka_repository import KafkaRepository
from entities.charging_message import ChargingMessage

CHARGING_EVENTS_TOPIC = os.getenv('CHARGING_EVENTS_TOPIC', 'charging_events')

class ChargingSessionService:
    def __init__(self, kafka_repository: KafkaRepository):
        self.kafka_repository = kafka_repository

    def charging_sessions(self, env, station_id: str, ev_battery_capacities: dict[str, int]):
        
        while True:
            
            # select random ev
            ev_id = random.choice(list(ev_battery_capacities.keys()))
            ev_battery_capacity = ev_battery_capacities[ev_id]
            # Wait random time before starting a new charging session
            inter_arrival_time = random.expovariate(1/5.0)
            yield env.timeout(inter_arrival_time)

            session_id = uuid.uuid4()

            # Start session
            self.start_charging_session(env, session_id, station_id, ev_id)
            
            # Charge
            yield from self.charge(env, session_id, station_id, ev_id, ev_battery_capacity)
            
            # End session
            self.end_charging_session(env, session_id, station_id, ev_id)

    def start_charging_session(self, env, session_id: uuid.UUID, station_id: str, ev_id: str):

        start_message = ChargingMessage(
            event_type="start",
            session_id=str(session_id),
            station_id=station_id,
            ev_id=ev_id,
            payload={
                "event_type": "start",
                "session_id": str(session_id), 
                "station_id": station_id,
                "ev_id": ev_id,
                "start_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
        )
        self.kafka_repository.publish(start_message, CHARGING_EVENTS_TOPIC)

    def end_charging_session(self, env, session_id: uuid.UUID, station_id: str, ev_id: str):

        end_message = ChargingMessage(
            event_type="end",
            session_id=str(session_id),
            station_id=station_id,
            ev_id=ev_id,
            payload={
                "event_type": "end",
                "session_id": str(session_id),
                "station_id": station_id,
                "ev_id": ev_id,
                "end_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
        )   
        self.kafka_repository.publish(end_message, CHARGING_EVENTS_TOPIC)
        
    def charge(self, env, session_id: uuid.UUID, station_id: str, ev_id: str, ev_battery_capacity: int):
        
        min_charging_duration = 1
        max_charging_duration = ev_battery_capacity
        
        # Simulate charging
        charging_duration = random.uniform(min_charging_duration, max_charging_duration)
        time_elapsed = 0.0
        step_time = 2.0

        while time_elapsed < charging_duration:
            yield env.timeout(step_time)
            time_elapsed += step_time
            progress = min(time_elapsed / charging_duration, 1.0)
            
            progress_message = ChargingMessage(
                event_type="progress",
                session_id=str(session_id),
                station_id=station_id,
                ev_id=ev_id,
                payload={
                    "event_type": "progress",
                    "session_id": str(session_id),
                    "station_id": station_id,
                    "ev_id": ev_id,
                    "current_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "progress_percent": round(progress * 100, 2)
                }
            )
            self.kafka_repository.publish(progress_message, CHARGING_EVENTS_TOPIC)