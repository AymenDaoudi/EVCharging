from pymongo import MongoClient

class ChargingStationsRepository:
    
    def __init__(self, mongo_client: MongoClient):
        self.mongo_client = mongo_client
        self.db = mongo_client.get_database("Db")
        self.collection = self.db.get_collection("ChargingStations")

    def get_charging_stations_ids(self) -> list[str]:
        cursor = self.collection.find({}, {"_id": 1})
        return [str(station["_id"]) for station in cursor]
