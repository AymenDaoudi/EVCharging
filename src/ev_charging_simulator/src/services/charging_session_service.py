from datetime import datetime
import os
import random
import uuid
import logging

from repositories.kafka_repositories.kafka_repository import KafkaRepository
from entities.charging_message import ChargingMessage

CHARGING_EVENTS_TOPIC = os.getenv('CHARGING_EVENTS_TOPIC', 'charging_events')

class ChargingSessionService:
    def __init__(self, kafka_repository: KafkaRepository):
        self.kafka_repository = kafka_repository
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        self.session_counter = 0  # Global counter for session numbers

    def get_next_session_number(self):
        """Generate a unique sequential session number"""
        self.session_counter += 1
        return self.session_counter

    def charging_sessions(self, env, station_id: str, ev_battery_capacities: dict[str, int]):
        """
        Simulates charging sessions for a charging station.
        """
        while True:
            # Generate a unique session ID
            session_id = str(uuid.uuid4())
            
            # Get a unique sequential session number
            session_number = self.get_next_session_number()
            
            # Randomly select an EV
            ev_id = random.choice(list(ev_battery_capacities.keys()))
            ev_battery_capacity = ev_battery_capacities[ev_id]
            
            # Simulate EV arrival
            arrival_message = ChargingMessage(
                session_id=session_id,
                session_number=session_number,
                station_id=station_id,
                ev_id=ev_id,
                event_type="start",
                payload={
                    "timestamp": datetime.now().isoformat(),
                }
            )
            
            # Publish arrival message
            self.kafka_repository.publish(arrival_message, CHARGING_EVENTS_TOPIC)
            
            # Simulate charging time (between 10 and 30 minutes)
            charging_time = random.uniform(2, 5)
            yield env.timeout(charging_time)
            
            # Simulate charging completion
            completion_message = ChargingMessage(
                session_id=session_id,
                session_number=session_number,
                station_id=station_id,
                ev_id=ev_id,
                event_type="stop",
                payload={
                    "timestamp": datetime.now().isoformat(),
                    "energy_delivered": str(random.uniform(10, ev_battery_capacity)),
                    "duration_minutes": str(charging_time)
                }
            )
            
            # Publish completion message
            self.kafka_repository.publish(completion_message, CHARGING_EVENTS_TOPIC)
            
            # Wait for next EV (between 5 and 15 minutes)
            yield env.timeout(random.uniform(1, 5))