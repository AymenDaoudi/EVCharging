import os

from pymongo import MongoClient
from repositories.db_repositories.charging_stations_repository import ChargingStationsRepository
from repositories.db_repositories.ev_repository import EVRepository
from repositories.kafka_repositories.kafka_config import KafkaConfig
from repositories.kafka_repositories.kafka_repository import KafkaRepository
from services.charging_session_service import ChargingSessionService
from services.simulation_service import SimulationService

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017/")

def main():
    
    kafka_config = KafkaConfig(KAFKA_BOOTSTRAP_SERVERS)
    mongo_client = MongoClient(MONGO_URI)
    
    kafka_repository = KafkaRepository(kafka_config)
    charging_stations_repository = ChargingStationsRepository(mongo_client)
    ev_repository = EVRepository(mongo_client)
    charging_session_service = ChargingSessionService(kafka_repository)
    
    simulation_service = SimulationService(
        kafka_repository,
        charging_stations_repository,
        ev_repository,
        charging_session_service
    )
    
    simulation_service.simulate()

if __name__ == "__main__":
    main()