const fs = require('fs');
const { v4: uuidv4 } = require('uuid');

class CSVParser {
    constructor(filePath) {
        this.csvData = fs.readFileSync(filePath, "utf-8").split("\n");
        this.headers = this.csvData[0].split(",");
    }

    parse() {
        let rows = [];
        
        for (let i = 1; i < this.csvData.length; i++) {
            let row = this.parseRow(this.csvData[i]);
            if (row.length !== this.headers.length) continue;
            rows.push(row);
        }
        
        return rows;
    }

    parseRow(line) {
        let row = [];
        let currentField = '';
        let stack = [];
        let chars = line.split('');
        
        for (let j = 0; j < chars.length; j++) {
            let char = chars[j];
            
            if (char === '"') {
                if (stack.length > 0) {
                    stack.pop();
                } else {
                    stack.push(char);
                }
                continue;
            }
            
            if (char === ',' && stack.length === 0) {
                row.push(currentField.trim());
                currentField = '';
                continue;
            }
            
            currentField += char;
        }
        
        // Push the last field
        row.push(currentField.trim());
        
        return row;
    }
}

// Initialize database
db.log.insertOne({ "message": "MongoDB is up and running!" });

db.log.insertOne({ "message": "Login as admin." });
db = db.getSiblingDB('admin');
db.auth(
    process.env.MONGO_INITDB_ROOT_USERNAME,
    process.env.MONGO_INITDB_ROOT_PASSWORD
)

db.log.insertOne({ "message": "Creating and switching to ${process.env.DB_NAME} database." });
db = db.getSiblingDB(process.env.DB_NAME);

db.log.insertOne({ "message": "Creating user for ${process.env.DB_NAME} database." });
db.createUser({
    user: process.env.DB_USER,
    pwd: process.env.DB_PASS,
    roles: [
        {
            role: "root",
            db: "admin"
        }
    ]
});

db.getSiblingDB(process.env.DB_NAME).auth(process.env.DB_USER, process.env.DB_PASS);

db.createCollection("ChargingStations");
db.createCollection("ElectricVehicles");

// Parse and insert charging stations
const stationsParser = new CSVParser("/data/charging_stations.csv");
const stationsData = stationsParser.parse();
const stations = stationsData.map(row => ({
    _id: uuidv4(),
    stationId: row[0],
    location: {
        type: "Point",
        coordinates: [parseFloat(row[2]), parseFloat(row[1])]
    },
    address: row[3],
    chargerType: row[4],
    costPerKwh: parseFloat(row[5]),
    availability: row[6],
    distanceToCity: parseFloat(row[7]),
    usageStats: parseInt(row[8]),
    operator: row[9],
    chargingCapacity: parseInt(row[10]),
    connectorTypes: row[11].split(", ").map(t => t.trim()),
    installationYear: parseInt(row[12]),
    renewableEnergySource: row[13] === "Yes",
    rating: parseFloat(row[14]),
    parkingSpots: parseInt(row[15]),
    maintenanceFrequency: row[16]
}));

// Parse and insert electric vehicles
const vehiclesParser = new CSVParser("/data/electric_vehicles_2021.csv");
const vehiclesData = vehiclesParser.parse();
const vehicles = vehiclesData.map(row => ({
    _id: uuidv4(),
    Battery_capacity: parseFloat(row[0]),
    Name: row[1],
    Efficiency: parseInt(row[3]),
    Fast_charge: parseInt(row[4]),
    Price: parseInt(row[5]),
    Range: parseInt(row[6]),
    Top_speed: parseInt(row[7]),
    Acceleration_0_100: parseFloat(row[8])
}));

// Insert the transformed data
db.ChargingStations.insertMany(stations);
db.ElectricVehicles.insertMany(vehicles);

db.log.insertOne({
    "message": `Successfully imported ${stations.length} charging stations and ${vehicles.length} electric vehicles` 
});

// Create indexes
db.ChargingStations.createIndex({ location: "2dsphere" }, { name: "coordinates_index" });
db.log.insertOne({"message": "Successfully created coordinates index"});

db.ChargingStations.createIndex({ stationId: 1 }, { unique: true, name: "stationId_index" });
db.log.insertOne({"message": "Successfully created stationId index"});

db.ElectricVehicles.createIndex({ Battery: 1 }, { name: "battery_index" });
db.log.insertOne({"message": "Successfully created battery power index"});