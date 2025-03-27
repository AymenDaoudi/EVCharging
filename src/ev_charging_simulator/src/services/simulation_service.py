import os
import simpy
from repositories.db_repositories.ev_repository import EVRepository
from repositories.db_repositories.charging_stations_repository import ChargingStationsRepository
from repositories.kafka_repositories.kafka_repository import KafkaRepository
from services.charging_session_service import ChargingSessionService

CHARGING_EVENTS_TOPIC = os.getenv('CHARGING_EVENTS_TOPIC', 'ev_charging_events')
SIM_DURATION = float(os.getenv('SIM_DURATION', 10))
CHARGING_STATIONS_TO_USE = int(os.getenv('CHARGING_STATIONS_TO_USE', 10))

class SimulationService:
    def __init__(self,
        kafka_repository: KafkaRepository, 
        charging_stations_repository: ChargingStationsRepository,
        ev_repository: EVRepository,
        charging_session_service: ChargingSessionService):
        
        self.kafka_repository = kafka_repository
        self.charging_stations_repository = charging_stations_repository
        self.ev_repository = ev_repository
        self.charging_session_service = charging_session_service

    def simulate(self):
        """
        Simulates the charging sessions for the charging stations.
        """
        
        print("Retrieving charging station ids...")
        # Get the charging stations
        charging_station_ids = self.charging_stations_repository.get_charging_stations_ids()
        
        print(f"Retrieved {len(charging_station_ids)} charging station ids")
        
        print("Ensuring topic exists...")
        self.kafka_repository.ensure_topic_exists(CHARGING_EVENTS_TOPIC)
        
        ev_battery_capacities = self.ev_repository.get_all_battery_capacities()
        
        print("Simulation started...")
        
        print("simpy environment created...")
        env = simpy.Environment() # type: ignore
        
        limited_charging_station_ids = charging_station_ids[:CHARGING_STATIONS_TO_USE]
        
        for station_id in limited_charging_station_ids:
            print(f"Starting charging session for station {station_id}...")
            env.process(
                self.charging_session_service.charging_sessions(
                    env, 
                    station_id,
                    ev_battery_capacities
                )
            )
        
        print("Simulation completed...")
        
        env.run(until=SIM_DURATION)
        
        # Flush Kafka messages at the end
        self.kafka_repository.kafka_config.producer.flush()
        print("Simulation complete.")