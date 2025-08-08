-- Create schema for meter data
CREATE SCHEMA IF NOT EXISTS meter_data;

-- Create table for meter readings
CREATE TABLE IF NOT EXISTS meter_data.readings (
    id SERIAL PRIMARY KEY,
    meter_id VARCHAR(50) NOT NULL,
    reading_value DECIMAL(15, 3) NOT NULL,
    interval_start TIMESTAMP NOT NULL,
    interval_end TIMESTAMP NOT NULL,
    creation_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    reading_type VARCHAR(20) NOT NULL,
    unit_of_measure VARCHAR(10) NOT NULL,
    quality_code VARCHAR(10),
    status VARCHAR(20) DEFAULT 'VALID'
);

-- Create index on meter_id and interval_start for faster queries
CREATE INDEX IF NOT EXISTS idx_readings_meter_id ON meter_data.readings(meter_id);
CREATE INDEX IF NOT EXISTS idx_readings_interval ON meter_data.readings(interval_start, interval_end);

-- Create table for meters
CREATE TABLE IF NOT EXISTS meter_data.meters (
    meter_id VARCHAR(50) PRIMARY KEY,
    installation_id VARCHAR(50) NOT NULL,
    meter_type VARCHAR(20) NOT NULL,
    installation_date TIMESTAMP NOT NULL,
    last_read_date TIMESTAMP,
    status VARCHAR(20) DEFAULT 'ACTIVE'
);

-- Add comments to tables and columns
COMMENT ON TABLE meter_data.readings IS 'Stores meter reading data with interval and creation timestamps';
COMMENT ON TABLE meter_data.meters IS 'Stores meter metadata';