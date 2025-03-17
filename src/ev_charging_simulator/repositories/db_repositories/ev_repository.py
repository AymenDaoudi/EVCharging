from pymongo import MongoClient

class EVRepository:
    
    def __init__(self, mongo_client: MongoClient):
        self.mongo_client = mongo_client
        self.db = mongo_client.get_database("Db")
        self.collection = self.db.get_collection("ElectricVehicles")

    def get_all_battery_capacities(self):
        cursor = self.collection.find({}, {"Battery_capacity": 1})
        return {str(ev["_id"]): int(ev["Battery_capacity"]) for ev in cursor}