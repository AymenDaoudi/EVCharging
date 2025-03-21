-- Create the database if it doesn't exist
CREATE DATABASE IF NOT EXISTS ev_charging;

-- Use the database
USE ev_charging;

-- Create dimension tables for the star schema

-- Time dimension table
CREATE TABLE IF NOT EXISTS dim_time
(
    time_id UInt32,
    hour UInt8,
    day UInt8,
    month UInt8,
    year UInt16,
    day_of_week UInt8,
    is_weekend UInt8,
    is_holiday UInt8
) ENGINE = MergeTree()
ORDER BY time_id;

-- Station dimension table
CREATE TABLE IF NOT EXISTS dim_station
(
    id UUID,
    station_name String,
    latitude Float32,
    longitude Float32,
    address String,
    charger_type String,
    costPerKwh Decimal(10, 2),
    distance_to_city Float32,
    operator String,
    charging_capacity UInt32,
    connector_type String,
    installation_year UInt32,
    renewable_energy_source Boolean,
    rating Float32,
    maintenance_frequency String,
    parking_spot UInt32
) ENGINE = MergeTree()
ORDER BY id;

-- Vehicle dimension table
CREATE TABLE IF NOT EXISTS dim_vehicle
(
    id UUID,
    model String,
    efficiency UInt32,
    fast_charge UInt32,
    range UInt32,
    top_speed UInt32,
    acceleration_0_100 UInt32,
    battery_capacity Float32
) ENGINE = MergeTree()
ORDER BY id;

-- Fact table for charging sessions
CREATE TABLE IF NOT EXISTS fact_charging_sessions
(
    session_id UUID,
    station_id UUID,
    vehicle_id UUID,
    start_time_id UInt32,
    end_time_id UInt32,
    energy_delivered Float32,
    duration_minutes Float32
) ENGINE = MergeTree()
ORDER BY (session_id);

-- Create a view for easy querying of the star schema
CREATE VIEW IF NOT EXISTS v_charging_sessions AS
SELECT
    f.session_id,
    f.start_time_id,
    f.end_time_id,
    f.energy_delivered,
    f.duration_minutes,
    f.station_id,
    s.station_name,
    s.latitude AS station_latitude,
    s.longitude AS station_longitude,
    s.address AS station_address,
    s.charger_type,
    s.costPerKwh AS charging_cost,
    s.distance_to_city,
    s.operator AS station_operator,
    s.charging_capacity,
    s.connector_type,
    s.installation_year,
    s.renewable_energy_source,
    s.rating AS station_rating,
    s.maintenance_frequency,
    f.vehicle_id,
    v.model AS vehicle_model,
    v.efficiency AS vehicle_efficiency,
    v.fast_charge AS vehicle_fast_charge,
    v.range AS vehicle_range,
    v.top_speed,
    v.acceleration_0_100,
    t_start.hour AS start_hour,
    t_start.day AS start_day,
    t_start.month AS start_month,
    t_start.year AS start_year,
    t_start.day_of_week AS start_day_of_week,
    t_start.is_weekend AS start_is_weekend,
    t_start.is_holiday AS start_is_holiday,
    t_end.hour AS end_hour,
    t_end.day AS end_day,
    t_end.month AS end_month,
    t_end.year AS end_year,
    t_end.day_of_week AS end_day_of_week,
    t_end.is_weekend AS end_is_weekend,
    t_end.is_holiday AS end_is_holiday
FROM fact_charging_sessions f
LEFT JOIN dim_station s ON f.station_id = s.id
LEFT JOIN dim_vehicle v ON f.vehicle_id = v.id
LEFT JOIN dim_time t_start ON f.start_time_id = t_start.time_id
LEFT JOIN dim_time t_end ON f.end_time_id = t_end.time_id; 