-- ============================================================================
-- TEST DATA for Data Vault 2.0 Demo
-- ============================================================================

-- ============================================================================
-- CUSTOMERS (Hub: Customer)
-- ============================================================================
INSERT INTO meter_data.customers (installation_id, customer_name, customer_type, address, city, postal_code, country, registration_date, status)
VALUES
    ('INST001', 'Acme Corporation', 'COMMERCIAL', '123 Main St, Suite 100', 'New York', '10001', 'USA', '2023-01-15 08:00:00', 'ACTIVE'),
    ('INST002', 'John Smith Residence', 'RESIDENTIAL', '456 Oak Avenue', 'Los Angeles', '90001', 'USA', '2023-03-20 10:00:00', 'ACTIVE'),
    ('INST003', 'Green Energy Facility', 'INDUSTRIAL', '789 Industrial Blvd', 'Chicago', '60601', 'USA', '2023-05-10 14:00:00', 'ACTIVE'),
    ('INST004', 'Downtown Plaza', 'COMMERCIAL', '321 Commerce Dr', 'Houston', '77001', 'USA', '2023-07-05 09:00:00', 'ACTIVE'),
    ('INST005', 'Tech Startup Inc', 'COMMERCIAL', '555 Innovation Way', 'San Francisco', '94102', 'USA', '2023-09-12 11:00:00', 'ACTIVE');

-- ============================================================================
-- CUSTOMER ACCOUNTS (Hub: Customer Account)
-- ============================================================================
INSERT INTO meter_data.customer_accounts (account_id, installation_id, account_number, account_type, billing_cycle, account_opened_date, status)
VALUES
    ('ACC001', 'INST001', 'A-2023-001', 'POSTPAID', 'MONTHLY', '2023-01-15 08:30:00', 'ACTIVE'),
    ('ACC002', 'INST002', 'A-2023-002', 'POSTPAID', 'MONTHLY', '2023-03-20 10:15:00', 'ACTIVE'),
    ('ACC003', 'INST003', 'A-2023-003', 'POSTPAID', 'MONTHLY', '2023-05-10 14:45:00', 'ACTIVE'),
    ('ACC004', 'INST004', 'A-2023-004', 'PREPAID', 'MONTHLY', '2023-07-05 09:00:00', 'CLOSED'),
    ('ACC005', 'INST005', 'A-2023-005', 'POSTPAID', 'MONTHLY', '2023-09-12 11:30:00', 'ACTIVE');

-- ============================================================================
-- ASSETS (Hub: Asset) - Physical meters
-- ============================================================================
INSERT INTO meter_data.assets (asset_id, account_id, asset_serial_number, asset_type, manufacturer, model, installation_date, last_calibration_date, next_calibration_date, status)
VALUES
    ('METER001', 'ACC001', 'SN-ELEC-001', 'ELECTRIC', 'Siemens', 'S7-1200', '2024-01-15 08:30:00', '2024-06-01 10:00:00', '2025-06-01 10:00:00', 'ACTIVE'),
    ('METER002', 'ACC002', 'SN-GAS-002', 'GAS', 'Honeywell', 'G4-RF1', '2024-02-20 10:15:00', '2024-08-01 09:00:00', '2025-08-01 09:00:00', 'ACTIVE'),
    ('METER003', 'ACC003', 'SN-WATER-003', 'WATER', 'Itron', 'W-100', '2024-03-10 14:45:00', '2024-09-01 11:00:00', '2025-09-01 11:00:00', 'ACTIVE'),
    ('METER004', 'ACC004', 'SN-ELEC-004', 'ELECTRIC', 'GE', 'I-210+', '2024-04-05 09:00:00', '2024-04-05 09:00:00', '2025-04-05 09:00:00', 'INACTIVE'),
    ('METER005', 'ACC005', 'SN-ELEC-005', 'ELECTRIC', 'Landis+Gyr', 'E650', '2024-05-12 11:30:00', '2024-11-01 10:00:00', '2025-11-01 10:00:00', 'ACTIVE');

-- ============================================================================
-- READINGS (Satellite: Asset Measurements)
-- ============================================================================

-- METER001 (Electric) - Hourly readings for the past 24 hours
INSERT INTO meter_data.readings (asset_id, reading_value, interval_start, interval_end, creation_time, reading_type, unit_of_measure, quality_code, status)
SELECT
    'METER001',
    100 + (random() * 50)::DECIMAL(15,3),
    '2025-10-02 10:00:00'::TIMESTAMP + (n || ' hours')::INTERVAL,
    '2025-10-02 10:00:00'::TIMESTAMP + ((n+1) || ' hours')::INTERVAL,
    '2025-10-02 10:00:00'::TIMESTAMP + (n || ' hours')::INTERVAL + (random() * 10 || ' minutes')::INTERVAL,
    'CONSUMPTION',
    'kWh',
    'GOOD',
    'VALID'
FROM generate_series(0, 23) AS n;

-- METER002 (Gas) - 4-hour intervals for the past 2 days
INSERT INTO meter_data.readings (asset_id, reading_value, interval_start, interval_end, creation_time, reading_type, unit_of_measure, quality_code, status)
SELECT
    'METER002',
    20 + (random() * 10)::DECIMAL(15,3),
    '2025-10-01 00:00:00'::TIMESTAMP + (n*4 || ' hours')::INTERVAL,
    '2025-10-01 00:00:00'::TIMESTAMP + ((n+1)*4 || ' hours')::INTERVAL,
    '2025-10-01 00:00:00'::TIMESTAMP + (n*4 || ' hours')::INTERVAL + (random() * 30 || ' minutes')::INTERVAL,
    'CONSUMPTION',
    'm³',
    CASE WHEN random() > 0.9 THEN 'SUSPECT' ELSE 'GOOD' END,
    CASE WHEN random() > 0.95 THEN 'ESTIMATED' ELSE 'VALID' END
FROM generate_series(0, 11) AS n;

-- METER003 (Water) - Daily intervals for the past week
INSERT INTO meter_data.readings (asset_id, reading_value, interval_start, interval_end, creation_time, reading_type, unit_of_measure, quality_code, status)
SELECT
    'METER003',
    500 + (random() * 100)::DECIMAL(15,3),
    '2025-09-26 00:00:00'::TIMESTAMP + (n || ' days')::INTERVAL,
    '2025-09-26 00:00:00'::TIMESTAMP + ((n+1) || ' days')::INTERVAL,
    '2025-09-26 00:00:00'::TIMESTAMP + (n || ' days')::INTERVAL + (random() * 120 || ' minutes')::INTERVAL,
    'CONSUMPTION',
    'L',
    CASE WHEN random() > 0.8 THEN 'SUSPECT' ELSE 'GOOD' END,
    CASE WHEN random() > 0.9 THEN 'ESTIMATED' ELSE 'VALID' END
FROM generate_series(0, 6) AS n;

-- METER004 (Inactive Electric) - Historical data (15-minute intervals for a single day)
INSERT INTO meter_data.readings (asset_id, reading_value, interval_start, interval_end, creation_time, reading_type, unit_of_measure, quality_code, status)
SELECT
    'METER004',
    75 + (random() * 25)::DECIMAL(15,3),
    '2025-09-26 00:00:00'::TIMESTAMP + (n*15 || ' minutes')::INTERVAL,
    '2025-09-26 00:00:00'::TIMESTAMP + ((n+1)*15 || ' minutes')::INTERVAL,
    '2025-09-26 00:00:00'::TIMESTAMP + (n*15 || ' minutes')::INTERVAL + (random() * 5 || ' minutes')::INTERVAL,
    'CONSUMPTION',
    'kWh',
    'GOOD',
    'VALID'
FROM generate_series(0, 95) AS n;

-- METER005 (Electric) - Peak demand readings with 15-minute intervals
INSERT INTO meter_data.readings (asset_id, reading_value, interval_start, interval_end, creation_time, reading_type, unit_of_measure, quality_code, status)
SELECT
    'METER005',
    CASE
        WHEN n BETWEEN 32 AND 40 THEN 200 + (random() * 100)::DECIMAL(15,3)  -- Peak hours (8am-10am)
        WHEN n BETWEEN 64 AND 76 THEN 250 + (random() * 150)::DECIMAL(15,3)  -- Peak hours (4pm-7pm)
        ELSE 50 + (random() * 50)::DECIMAL(15,3)                            -- Off-peak
    END,
    '2025-10-02 00:00:00'::TIMESTAMP + (n*15 || ' minutes')::INTERVAL,
    '2025-10-02 00:00:00'::TIMESTAMP + ((n+1)*15 || ' minutes')::INTERVAL,
    '2025-10-02 00:00:00'::TIMESTAMP + (n*15 || ' minutes')::INTERVAL + (random() * 3 || ' minutes')::INTERVAL,
    CASE WHEN n % 4 = 0 THEN 'DEMAND' ELSE 'CONSUMPTION' END,
    'kWh',
    CASE WHEN random() > 0.95 THEN 'SUSPECT' ELSE 'GOOD' END,
    CASE WHEN random() > 0.97 THEN 'ESTIMATED' ELSE 'VALID' END
FROM generate_series(0, 95) AS n;

-- ============================================================================
-- SUMMARY
-- ============================================================================
-- Display summary of inserted data
DO $$
DECLARE
    customer_count INT;
    account_count INT;
    asset_count INT;
    reading_count INT;
BEGIN
    SELECT COUNT(*) INTO customer_count FROM meter_data.customers;
    SELECT COUNT(*) INTO account_count FROM meter_data.customer_accounts;
    SELECT COUNT(*) INTO asset_count FROM meter_data.assets;
    SELECT COUNT(*) INTO reading_count FROM meter_data.readings;

    RAISE NOTICE '========================================';
    RAISE NOTICE 'Test Data Inserted Successfully';
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Customers: %', customer_count;
    RAISE NOTICE 'Accounts: %', account_count;
    RAISE NOTICE 'Assets (Meters): %', asset_count;
    RAISE NOTICE 'Readings: %', reading_count;
    RAISE NOTICE '========================================';
END $$;
