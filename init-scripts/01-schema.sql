-- Create schema for meter data
CREATE SCHEMA IF NOT EXISTS meter_data;

-- ============================================================================
-- CUSTOMER TABLE (Hub: Customer)
-- ============================================================================
CREATE TABLE IF NOT EXISTS meter_data.customers (
    installation_id VARCHAR(50) PRIMARY KEY,
    customer_name VARCHAR(100) NOT NULL,
    customer_type VARCHAR(20) NOT NULL,  -- RESIDENTIAL, COMMERCIAL, INDUSTRIAL
    address VARCHAR(200),
    city VARCHAR(100),
    postal_code VARCHAR(20),
    country VARCHAR(50) DEFAULT 'USA',
    registration_date TIMESTAMP NOT NULL,
    last_modified TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(20) DEFAULT 'ACTIVE'
);

CREATE INDEX IF NOT EXISTS idx_customers_status ON meter_data.customers(status);
CREATE INDEX IF NOT EXISTS idx_customers_type ON meter_data.customers(customer_type);

COMMENT ON TABLE meter_data.customers IS 'Stores customer/installation information';

-- ============================================================================
-- CUSTOMER ACCOUNT TABLE (Hub: Customer Account)
-- ============================================================================
CREATE TABLE IF NOT EXISTS meter_data.customer_accounts (
    account_id VARCHAR(50) PRIMARY KEY,
    installation_id VARCHAR(50) NOT NULL,  -- FK to customers
    account_number VARCHAR(50) NOT NULL UNIQUE,
    account_type VARCHAR(20) NOT NULL,  -- PREPAID, POSTPAID
    billing_cycle VARCHAR(20),  -- MONTHLY, QUARTERLY
    account_opened_date TIMESTAMP NOT NULL,
    account_closed_date TIMESTAMP,
    last_modified TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(20) DEFAULT 'ACTIVE'
);

CREATE INDEX IF NOT EXISTS idx_accounts_installation ON meter_data.customer_accounts(installation_id);
CREATE INDEX IF NOT EXISTS idx_accounts_status ON meter_data.customer_accounts(status);
CREATE INDEX IF NOT EXISTS idx_accounts_number ON meter_data.customer_accounts(account_number);

COMMENT ON TABLE meter_data.customer_accounts IS 'Stores customer account information';

-- ============================================================================
-- ASSET TABLE (Hub: Asset) - Physical meters
-- ============================================================================
CREATE TABLE IF NOT EXISTS meter_data.assets (
    asset_id VARCHAR(50) PRIMARY KEY,
    account_id VARCHAR(50) NOT NULL,  -- FK to customer_accounts
    asset_serial_number VARCHAR(50) NOT NULL UNIQUE,
    asset_type VARCHAR(20) NOT NULL,  -- ELECTRIC, GAS, WATER
    manufacturer VARCHAR(50),
    model VARCHAR(50),
    installation_date TIMESTAMP NOT NULL,
    last_calibration_date TIMESTAMP,
    next_calibration_date TIMESTAMP,
    last_modified TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(20) DEFAULT 'ACTIVE'
);

CREATE INDEX IF NOT EXISTS idx_assets_account ON meter_data.assets(account_id);
CREATE INDEX IF NOT EXISTS idx_assets_serial ON meter_data.assets(asset_serial_number);
CREATE INDEX IF NOT EXISTS idx_assets_type ON meter_data.assets(asset_type);
CREATE INDEX IF NOT EXISTS idx_assets_status ON meter_data.assets(status);

COMMENT ON TABLE meter_data.assets IS 'Stores physical asset (meter) information';

-- ============================================================================
-- READINGS TABLE (Satellite: Asset Measurements)
-- ============================================================================
CREATE TABLE IF NOT EXISTS meter_data.readings (
    id SERIAL PRIMARY KEY,
    asset_id VARCHAR(50) NOT NULL,  -- FK to assets
    reading_value DECIMAL(15, 3) NOT NULL,
    interval_start TIMESTAMP NOT NULL,
    interval_end TIMESTAMP NOT NULL,
    creation_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    reading_type VARCHAR(20) NOT NULL,  -- CONSUMPTION, DEMAND
    unit_of_measure VARCHAR(10) NOT NULL,  -- kWh, m³, L
    quality_code VARCHAR(10),  -- GOOD, SUSPECT
    status VARCHAR(20) DEFAULT 'VALID',  -- VALID, ESTIMATED
    source_system VARCHAR(50) DEFAULT 'METER_SYSTEM'
);

CREATE INDEX IF NOT EXISTS idx_readings_asset_id ON meter_data.readings(asset_id);
CREATE INDEX IF NOT EXISTS idx_readings_interval ON meter_data.readings(interval_start, interval_end);
CREATE INDEX IF NOT EXISTS idx_readings_creation ON meter_data.readings(creation_time);

COMMENT ON TABLE meter_data.readings IS 'Stores meter reading measurements with timestamps';

-- ============================================================================
-- LEGACY TABLE (for backward compatibility during migration)
-- ============================================================================
-- Keep old meters table as a view for backward compatibility
CREATE OR REPLACE VIEW meter_data.meters AS
SELECT
    a.asset_id as meter_id,
    ca.installation_id,
    a.asset_type as meter_type,
    a.installation_date,
    MAX(r.interval_end) as last_read_date,
    a.status
FROM meter_data.assets a
LEFT JOIN meter_data.customer_accounts ca ON a.account_id = ca.account_id
LEFT JOIN meter_data.readings r ON a.asset_id = r.asset_id
GROUP BY a.asset_id, ca.installation_id, a.asset_type, a.installation_date, a.status;

COMMENT ON VIEW meter_data.meters IS 'Legacy view for backward compatibility - maps assets to old meter structure';