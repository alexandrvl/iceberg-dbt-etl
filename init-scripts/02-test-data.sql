-- Insert test data for meters
INSERT INTO meter_data.meters (meter_id, installation_id, meter_type, installation_date, last_read_date, status)
VALUES
    ('METER001', 'INST001', 'ELECTRIC', '2024-01-15 08:30:00', '2025-08-07 23:45:00', 'ACTIVE'),
    ('METER002', 'INST002', 'GAS', '2024-02-20 10:15:00', '2025-08-07 22:30:00', 'ACTIVE'),
    ('METER003', 'INST003', 'WATER', '2024-03-10 14:45:00', '2025-08-07 21:15:00', 'ACTIVE'),
    ('METER004', 'INST004', 'ELECTRIC', '2024-04-05 09:00:00', '2025-08-07 20:00:00', 'INACTIVE'),
    ('METER005', 'INST005', 'ELECTRIC', '2024-05-12 11:30:00', '2025-08-07 19:45:00', 'ACTIVE');

-- Insert test data for meter readings
-- Using the current date (2025-08-08) as reference for recent readings

-- METER001 - Hourly readings for the past 24 hours
INSERT INTO meter_data.readings (meter_id, reading_value, interval_start, interval_end, creation_time, reading_type, unit_of_measure, quality_code, status)
SELECT
    'METER001',
    100 + (random() * 50)::DECIMAL(15,3),
    '2025-08-07 10:00:00'::TIMESTAMP + (n || ' hours')::INTERVAL,
    '2025-08-07 10:00:00'::TIMESTAMP + ((n+1) || ' hours')::INTERVAL,
    '2025-08-07 10:00:00'::TIMESTAMP + (n || ' hours')::INTERVAL + (random() * 10 || ' minutes')::INTERVAL,
    'CONSUMPTION',
    'kWh',
    'GOOD',
    'VALID'
FROM generate_series(0, 23) AS n;

-- METER002 - Gas readings with 4-hour intervals for the past 2 days
INSERT INTO meter_data.readings (meter_id, reading_value, interval_start, interval_end, creation_time, reading_type, unit_of_measure, quality_code, status)
SELECT
    'METER002',
    20 + (random() * 10)::DECIMAL(15,3),
    '2025-08-06 00:00:00'::TIMESTAMP + (n*4 || ' hours')::INTERVAL,
    '2025-08-06 00:00:00'::TIMESTAMP + ((n+1)*4 || ' hours')::INTERVAL,
    '2025-08-06 00:00:00'::TIMESTAMP + (n*4 || ' hours')::INTERVAL + (random() * 30 || ' minutes')::INTERVAL,
    'CONSUMPTION',
    'm³',
    CASE WHEN random() > 0.9 THEN 'SUSPECT' ELSE 'GOOD' END,
    CASE WHEN random() > 0.95 THEN 'ESTIMATED' ELSE 'VALID' END
FROM generate_series(0, 11) AS n;

-- METER003 - Water readings with daily intervals for the past week
INSERT INTO meter_data.readings (meter_id, reading_value, interval_start, interval_end, creation_time, reading_type, unit_of_measure, quality_code, status)
SELECT
    'METER003',
    500 + (random() * 100)::DECIMAL(15,3),
    '2025-08-01 00:00:00'::TIMESTAMP + (n || ' days')::INTERVAL,
    '2025-08-01 00:00:00'::TIMESTAMP + ((n+1) || ' days')::INTERVAL,
    '2025-08-01 00:00:00'::TIMESTAMP + (n || ' days')::INTERVAL + (random() * 120 || ' minutes')::INTERVAL,
    'CONSUMPTION',
    'L',
    CASE WHEN random() > 0.8 THEN 'SUSPECT' ELSE 'GOOD' END,
    CASE WHEN random() > 0.9 THEN 'ESTIMATED' ELSE 'VALID' END
FROM generate_series(0, 6) AS n;

-- METER004 - Inactive meter with some historical data (15-minute intervals for a single day)
INSERT INTO meter_data.readings (meter_id, reading_value, interval_start, interval_end, creation_time, reading_type, unit_of_measure, quality_code, status)
SELECT
    'METER004',
    75 + (random() * 25)::DECIMAL(15,3),
    '2025-08-01 00:00:00'::TIMESTAMP + (n*15 || ' minutes')::INTERVAL,
    '2025-08-01 00:00:00'::TIMESTAMP + ((n+1)*15 || ' minutes')::INTERVAL,
    '2025-08-01 00:00:00'::TIMESTAMP + (n*15 || ' minutes')::INTERVAL + (random() * 5 || ' minutes')::INTERVAL,
    'CONSUMPTION',
    'kWh',
    'GOOD',
    'VALID'
FROM generate_series(0, 95) AS n;

-- METER005 - Electric meter with some peak demand readings
INSERT INTO meter_data.readings (meter_id, reading_value, interval_start, interval_end, creation_time, reading_type, unit_of_measure, quality_code, status)
SELECT
    'METER005',
    CASE 
        WHEN n BETWEEN 32 AND 40 THEN 200 + (random() * 100)::DECIMAL(15,3)  -- Peak hours (8am-10am)
        WHEN n BETWEEN 64 AND 76 THEN 250 + (random() * 150)::DECIMAL(15,3)  -- Peak hours (4pm-7pm)
        ELSE 50 + (random() * 50)::DECIMAL(15,3)                            -- Off-peak
    END,
    '2025-08-07 00:00:00'::TIMESTAMP + (n*15 || ' minutes')::INTERVAL,
    '2025-08-07 00:00:00'::TIMESTAMP + ((n+1)*15 || ' minutes')::INTERVAL,
    '2025-08-07 00:00:00'::TIMESTAMP + (n*15 || ' minutes')::INTERVAL + (random() * 3 || ' minutes')::INTERVAL,
    CASE WHEN n % 4 = 0 THEN 'DEMAND' ELSE 'CONSUMPTION' END,
    'kWh',
    CASE WHEN random() > 0.95 THEN 'SUSPECT' ELSE 'GOOD' END,
    CASE WHEN random() > 0.97 THEN 'ESTIMATED' ELSE 'VALID' END
FROM generate_series(0, 95) AS n;