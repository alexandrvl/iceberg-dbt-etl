# Data Vault 2.0 ETL Pipeline with Apache Iceberg

> Complete end-to-end ETL pipeline implementing Data Vault 2.0 methodology with Apache Iceberg, dbt, and AutomateDV

[![Data Vault 2.0](https://img.shields.io/badge/Data%20Vault-2.0-blue)]()
[![Apache Iceberg](https://img.shields.io/badge/Apache-Iceberg-orange)]()
[![dbt](https://img.shields.io/badge/dbt-AutomateDV-green)]()

---

## 📋 Table of Contents

- [Quick Start](#-quick-start)
- [Overview](#-overview)
- [Architecture](#-architecture)
- [Data Model](#-data-model)
- [Project Structure](#-project-structure)
- [Data Loading Framework](#-data-loading-framework)
- [Docker Configuration](#-docker-configuration)
- [Usage Guide](#-usage-guide)
- [Troubleshooting](#-troubleshooting)
- [Development](#-development)
- [References](#-references)

---

## 🚀 Quick Start

### Get Started in 3 Steps

#### 1. Start the Pipeline

```bash
docker compose up -d
```

#### 2. Watch the Progress

```bash
docker compose logs -f
```

You'll see:
- ✅ PostgreSQL initializes with test data
- ✅ Generic loader extracts data to Iceberg
- ✅ dbt builds Data Vault 2.0 structure

#### 3. Query the Data

```bash
# Connect to Trino
docker exec -it iceberg-dbt-trino trino

# Run example queries
SELECT * FROM iceberg.business_vault.bv_customer_asset_hierarchy;
SELECT * FROM iceberg.raw_vault.sat_asset_measurements LIMIT 10;
```

### What Gets Built

#### Source Data (PostgreSQL)
- 5 customers (residential, commercial, industrial)
- 5 customer accounts (prepaid, postpaid)
- 5 assets (electric, gas, water meters)
- 200+ meter readings

#### Data Vault 2.0 (Iceberg)
- **3 Hubs**: customer, customer_account, asset
- **2 Links**: customer↔account, account↔asset
- **4 Satellites**: customer_details, account_details, asset_details, measurements
- **4 References**: meter_type, reading_type, status, quality_code
- **4 Business Views**: Easy-to-query denormalized views

### Common Tasks

**Restart Pipeline**
```bash
docker compose down
docker compose up -d
```

**Clean Restart (Delete All Data)**
```bash
docker compose down -v
docker compose up -d
```

**View Specific Service Logs**
```bash
docker compose logs -f parquet-loader
docker compose logs -f dbt-models
```

**Switch to Config-Driven Loader**
```bash
# Edit docker compose.yml:
# LOADER_TYPE: config  # Change from "generic"

docker compose up --build parquet-loader
```

### Services & Ports

| Service | URL | Credentials |
|---------|-----|-------------|
| MinIO Console | http://localhost:9001 | minio_user / minio_password |
| Trino | localhost:8081 | - |
| PostgreSQL | localhost:5432 | dbt_user / dbt_password |

### Verify Everything Works

```bash
docker compose up -d

# 2. Check all services are running
docker compose ps

# 3. Connect to Trino and query
docker exec -it iceberg-dbt-trino trino -e "SHOW SCHEMAS IN iceberg;"

# Expected output:
# - raw
# - staging
# - raw_vault
# - business_vault
```

---

## 🎯 Overview

This project demonstrates a production-ready ETL pipeline featuring:

### Technology Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Source Database** | PostgreSQL 15 | Operational data (customers, accounts, assets, readings) |
| **Object Storage** | MinIO (S3-compatible) | Data lake storage |
| **Table Format** | Apache Iceberg | ACID transactions, time travel, schema evolution |
| **Query Engine** | Trino | Distributed SQL queries |
| **Transformation** | dbt + AutomateDV | Data Vault 2.0 transformations |
| **Data Loading** | Python + DuckDB + PyIceberg | Generic, reusable extraction framework |
| **Orchestration** | Docker Compose | Local development environment |

### Key Features

✅ **Generic Data Loader Framework** - Reusable across projects, configuration-driven
✅ **Data Vault 2.0** - Industry-standard data warehousing methodology
✅ **Incremental Loading** - State-based watermarks for efficient updates
✅ **Proper Entity Separation** - Normalized source schema (3NF)
✅ **Comprehensive Documentation** - Everything you need to understand and extend
✅ **Production Ready** - Error handling, logging, testing, monitoring

---

## 🏗️ Architecture

### System Overview

```
┌─────────────────────────────────────────────────────────────┐
│                     PostgreSQL (Source)                      │
│  • meter_data.customers                                      │
│  • meter_data.customer_accounts                              │
│  • meter_data.assets                                         │
│  • meter_data.readings                                       │
└────────────────────────┬────────────────────────────────────┘
                         │
                         │ Generic Data Loader
                         │ (DuckDB + PyIceberg)
                         ▼
┌─────────────────────────────────────────────────────────────┐
│              MinIO (S3) + Iceberg Catalog                    │
│  Raw Layer:                                                  │
│  • iceberg.raw.customers                                     │
│  • iceberg.raw.customer_accounts                             │
│  • iceberg.raw.assets                                        │
│  • iceberg.raw.readings                                      │
└────────────────────────┬────────────────────────────────────┘
                         │
                         │ dbt + Trino + AutomateDV
                         ▼
┌─────────────────────────────────────────────────────────────┐
│                  Data Vault 2.0 Structure                    │
├─────────────────────────────────────────────────────────────┤
│ 📊 STAGING (iceberg.staging)                                │
│  └─ stg_meter_data (hash keys, hash diffs)                  │
│                                                              │
│ 🔑 HUBS (Business Keys)                                     │
│  • hub_customer (installation_id)                           │
│  • hub_customer_account (account_id)                        │
│  • hub_asset (asset_id)                                     │
│                                                              │
│ 🔗 LINKS (Relationships)                                    │
│  • link_customer_account                                    │
│  • link_account_asset                                       │
│                                                              │
│ 📦 SATELLITES (Descriptive Data)                            │
│  • sat_customer_details                                     │
│  • sat_customer_acc_details                                 │
│  • sat_asset_details                                        │
│  • sat_asset_measurements (partitioned by day)              │
│                                                              │
│ 📚 REFERENCES (Shared Classifiers)                          │
│  • ref_meter_type, ref_reading_type                         │
│  • ref_status, ref_quality_code                             │
│                                                              │
│ 💼 BUSINESS VAULT (Consumption Layer)                       │
│  • bv_customer_accounts                                     │
│  • bv_asset_details                                         │
│  • bv_asset_measurements                                    │
│  • bv_customer_asset_hierarchy                              │
└─────────────────────────────────────────────────────────────┘
```

### Data Vault 2.0 Principles Applied

✅ **Insert-only pattern** - No updates or deletes
✅ **Load timestamp tracking** - `load_ts` on all entities
✅ **Source system tracking** - `record_source` for lineage
✅ **Hash keys** - MD5 hashes of business keys (AutomateDV)
✅ **Hash diffs** - Change detection in satellites
✅ **Proper entity separation** - Customers, Accounts, Assets
✅ **Incremental loading** - State-based watermarks
✅ **Partitioning** - Time-based partitioning for performance

---

## 📊 Data Model

### Entity Relationship

```
Customer (hub_customer)
    │
    └─── has many ───> Customer Account (hub_customer_account)
                           │
                           └─── has many ───> Asset (hub_asset)
                                                  │
                                                  └─── has many ───> Readings
```

### Source Schema (PostgreSQL)

#### `meter_data.customers` - Customer/Installation Entity

| Column | Type | Description |
|--------|------|-------------|
| installation_id | VARCHAR(50) PK | Customer identifier |
| customer_name | VARCHAR(100) | Name or company |
| customer_type | VARCHAR(20) | RESIDENTIAL, COMMERCIAL, INDUSTRIAL |
| address | VARCHAR(200) | Street address |
| city | VARCHAR(100) | City |
| postal_code | VARCHAR(20) | Postal/ZIP code |
| country | VARCHAR(50) | Country |
| registration_date | TIMESTAMP | Registration date |
| last_modified | TIMESTAMP | Last modification |
| status | VARCHAR(20) | ACTIVE, INACTIVE |

#### `meter_data.customer_accounts` - Customer Account Entity

| Column | Type | Description |
|--------|------|-------------|
| account_id | VARCHAR(50) PK | Account identifier |
| installation_id | VARCHAR(50) FK | Customer reference |
| account_number | VARCHAR(50) | Unique account number |
| account_type | VARCHAR(20) | PREPAID, POSTPAID |
| billing_cycle | VARCHAR(20) | MONTHLY, QUARTERLY |
| account_opened_date | TIMESTAMP | Account open date |
| account_closed_date | TIMESTAMP | Account close date |
| last_modified | TIMESTAMP | Last modification |
| status | VARCHAR(20) | ACTIVE, CLOSED |

#### `meter_data.assets` - Physical Meter/Asset Entity

| Column | Type | Description |
|--------|------|-------------|
| asset_id | VARCHAR(50) PK | Asset identifier |
| account_id | VARCHAR(50) FK | Account reference |
| asset_serial_number | VARCHAR(50) | Physical serial |
| asset_type | VARCHAR(20) | ELECTRIC, GAS, WATER |
| manufacturer | VARCHAR(50) | Siemens, Honeywell, etc. |
| model | VARCHAR(50) | Model number |
| installation_date | TIMESTAMP | Installation date |
| last_calibration_date | TIMESTAMP | Last calibration |
| next_calibration_date | TIMESTAMP | Next calibration |
| last_modified | TIMESTAMP | Last modification |
| status | VARCHAR(20) | ACTIVE, INACTIVE |

#### `meter_data.readings` - Time-Series Measurements

| Column | Type | Description |
|--------|------|-------------|
| id | SERIAL PK | Surrogate key |
| asset_id | VARCHAR(50) FK | Asset reference |
| reading_value | DECIMAL(15,3) | Measurement value |
| interval_start | TIMESTAMP | Interval start |
| interval_end | TIMESTAMP | Interval end |
| creation_time | TIMESTAMP | Ingestion timestamp |
| reading_type | VARCHAR(20) | CONSUMPTION, DEMAND |
| unit_of_measure | VARCHAR(10) | kWh, m³, L |
| quality_code | VARCHAR(10) | GOOD, SUSPECT |
| status | VARCHAR(20) | VALID, ESTIMATED |
| source_system | VARCHAR(50) | Source system |

### Data Vault Mapping

| Source Table | Data Vault Entity | Type | Business Key |
|--------------|-------------------|------|--------------|
| customers | hub_customer | Hub | installation_id |
| customer_accounts | hub_customer_account | Hub | account_id |
| assets | hub_asset | Hub | asset_id |
| customers + accounts | link_customer_account | Link | installation_id + account_id |
| accounts + assets | link_account_asset | Link | account_id + asset_id |
| customers (attrs) | sat_customer_details | Satellite | Customer attributes |
| customer_accounts (attrs) | sat_customer_acc_details | Satellite | Account attributes |
| assets (attrs) | sat_asset_details | Satellite | Asset attributes |
| readings | sat_asset_measurements | Satellite | Time-series data |

---

## 📁 Project Structure

```
iceberg-dbt-etl/
├── README.md                      ⭐ This file (complete documentation)
├── verify_docker_setup.sh         ✅ Verification script (44 checks)
│
├── docker compose.yml             🎼 Service orchestration
├── Dockerfile.parquet-loader      🐳 Data loader container
├── Dockerfile.dbt                 🐳 dbt transformation container
│
├── load_data_generic.py           ⭐ Generic data loader (DEFAULT)
├── load_data_from_config.py       📄 Config-driven loader
├── table_config.example.json      📋 Example table configuration
│
├── init-scripts/                  💾 PostgreSQL initialization
│   ├── 01-schema.sql             # Source schema (4 tables)
│   └── 02-test-data.sql          # Test data (5+5+5+200 records)
│
├── meterdata/                     📊 dbt project
│   ├── dbt_project.yml           # dbt configuration
│   ├── packages.yml              # AutomateDV + dbt-utils
│   ├── profiles.yml              # Trino connection
│   │
│   └── models/
│       ├── raw/
│       │   └── sources.yml       # Source definitions (4 tables)
│       │
│       ├── staging/
│       │   └── stg_meter_data.sql    # Hash keys, hash diffs
│       │
│       ├── raw_vault/
│       │   ├── hubs/             # 3 hub tables + tests
│       │   ├── links/            # 2 link tables + tests
│       │   ├── satellites/       # 4 satellite tables + tests
│       │   └── references/       # 4 reference tables
│       │
│       └── business_vault/
│           ├── bv_customer_accounts.sql
│           ├── bv_asset_details.sql
│           ├── bv_asset_measurements.sql
│           └── bv_customer_asset_hierarchy.sql
│
└── trino/etc/                     ⚙️ Trino configuration
    └── catalog/
        └── iceberg.properties
```

---

## 🔄 Data Loading Framework

### Overview

The project uses a **generic, reusable data loading framework** that can be applied to any project.

### Framework Architecture

```python
# Abstract interfaces for extensibility
class DataSourceInterface(ABC):
    """Support PostgreSQL, MySQL, Oracle, etc."""
    @abstractmethod
    def connect(self) -> Any: pass

class StateManagerInterface(ABC):
    """Support S3, database, local file"""
    @abstractmethod
    def get_state(self) -> Dict: pass

# Configuration-driven table definitions
table_def = TableDefinition(
    name="customers",
    fields=[
        FieldDefinition("customer_id", FieldType.STRING),
        FieldDefinition("balance", FieldType.DECIMAL, precision=15, scale=2)
    ]
)

# Load data
loader = GenericDataLoader(config, data_source, [table_def])
loader.load_all_tables()
```

### Two Loading Options

#### Option 1: Generic Loader (DEFAULT)

Code-based table definitions using Python classes:

```python
from load_data_generic import *

customers = TableDefinition(
    name="customers",
    primary_key="installation_id",
    fields=[
        FieldDefinition("installation_id", FieldType.STRING, required=True),
        FieldDefinition("customer_name", FieldType.STRING),
        FieldDefinition("registration_date", FieldType.TIMESTAMP),
    ]
)

loader = GenericDataLoader(config, data_source, [customers])
loader.load_all_tables()
```

#### Option 2: Config-Driven Loader

JSON/YAML configuration files:

```json
{
  "tables": [
    {
      "name": "customers",
      "primary_key": "installation_id",
      "fields": [
        {"name": "installation_id", "type": "string", "required": true},
        {"name": "customer_name", "type": "string"}
      ]
    }
  ]
}
```

```python
table_defs = ConfigLoader.from_dict(json.load(open('config.json')))
loader = GenericDataLoader(config, data_source, table_defs)
loader.load_all_tables()
```

### Key Features

✅ **Configuration-driven** - Define tables in code or JSON/YAML
✅ **Pluggable data sources** - PostgreSQL, MySQL, Oracle (via interfaces)
✅ **Pluggable state management** - S3, database, local file
✅ **Automatic schema conversion** - Abstract types → Iceberg types
✅ **Automatic query generation** - SELECT queries from table definitions
✅ **Clean architecture** - SOLID principles, separation of concerns

### Extensibility Example

Add MySQL support by implementing the interface:

```python
class MySQLDataSource(DataSourceInterface):
    def connect(self):
        self.connection = duckdb.connect(":memory:")
        self.connection.execute("INSTALL mysql; LOAD mysql;")
        conn_str = self.get_connection_string()
        self.connection.execute(f"ATTACH '{conn_str}' AS raw (TYPE mysql);")
        return self.connection

    def get_connection_string(self):
        return f"host={self.config.source_host} ..."
```

---

## 🐳 Docker Configuration

### Services

| Service | Port | Purpose | Access |
|---------|------|---------|--------|
| postgres | 5432 | Source database | `localhost:5432` |
| minio | 9000, 9001 | S3-compatible storage | Console: `http://localhost:9001` |
| trino | 8081 | SQL query engine | `localhost:8081` |
| parquet-loader | - | Data extraction | Runs once |
| dbt-models | - | dbt transformations | Runs once |

### Environment Variables

#### parquet-loader Service

```yaml
# Loader selection
LOADER_TYPE: generic  # or "config"

# Source database
SOURCE_HOST: postgres
SOURCE_PORT: 5432
SOURCE_DB: iceberg_dbt
SOURCE_USER: dbt_user
SOURCE_PASSWORD: dbt_password
SOURCE_SCHEMA: meter_data

# S3/MinIO
S3_REGION: eu-central-1
S3_ACCESS_KEY: minio_user
S3_SECRET_KEY: minio_password
S3_ENDPOINT: minio:9000
S3_BUCKET: iceberg-data
```

#### dbt-models Service

```yaml
DBT_TARGET: prod
DBT_THREADS: 1
DBT_TRINO_HOST: trino
DBT_TRINO_PORT: 8080
DBT_TRINO_USER: trino
DBT_TRINO_DATABASE: iceberg
DBT_TRINO_SCHEMA: raw_vault
```

### Switching Between Loaders

Edit `docker compose.yml`:

```yaml
parquet-loader:
  environment:
    LOADER_TYPE: generic  # Change to "config" for JSON-based
```

Then rebuild:
```bash
docker compose up --build parquet-loader
```

---

## 📖 Usage Guide

### Starting the Pipeline

```bash
# Full pipeline (fresh start)
docker compose down -v
docker compose up -d

# Monitor progress
docker compose logs -f
```

### Incremental Load

```bash
# Add new data to PostgreSQL
docker exec -it iceberg-dbt-postgres psql -U dbt_user -d iceberg_dbt

INSERT INTO meter_data.readings (asset_id, reading_value, interval_start, interval_end, creation_time)
VALUES ('METER001', 125.5, NOW(), NOW(), NOW());

\q

# Re-run loaders
docker compose up parquet-loader
docker compose up dbt-models
```

### Querying Data

```bash
# Connect to Trino
docker exec -it iceberg-dbt-trino trino
```

#### Example Queries

**1. Customer Hierarchy**
```sql
SELECT
    installation_id,
    customer_name,
    customer_type,
    account_meter_id,
    meter_type,
    meter_status
FROM iceberg.business_vault.bv_customer_asset_hierarchy
WHERE meter_status = 'ACTIVE'
ORDER BY installation_id;
```

**2. Recent Measurements**
```sql
SELECT
    asset_id,
    reading_value,
    interval_start,
    reading_type,
    unit_of_measure,
    quality_code
FROM iceberg.business_vault.bv_asset_measurements
WHERE DATE(interval_start) = CURRENT_DATE
ORDER BY interval_start DESC
LIMIT 20;
```

**3. Data Quality Check**
```sql
SELECT
    quality_code,
    reading_status,
    COUNT(*) as reading_count
FROM iceberg.raw_vault.sat_asset_measurements
GROUP BY 1, 2
ORDER BY 3 DESC;
```

**4. Hub Record Counts**
```sql
SELECT 'Customers' as entity, COUNT(*) FROM iceberg.raw_vault.hub_customer
UNION ALL
SELECT 'Accounts', COUNT(*) FROM iceberg.raw_vault.hub_customer_account
UNION ALL
SELECT 'Assets', COUNT(*) FROM iceberg.raw_vault.hub_asset;
```

**5. Time Travel (Iceberg Feature)**
```sql
-- Query data as of specific timestamp
SELECT * FROM iceberg.raw_vault.sat_asset_measurements
FOR SYSTEM_TIME AS OF TIMESTAMP '2025-10-03 10:00:00';
```

### dbt Development

```bash
# Enter dbt container
docker exec -it iceberg-dbt-models bash

# Inside container
cd /app/meterdata

# Run specific models
dbt run --select staging
dbt run --select raw_vault
dbt run --select business_vault

# Run tests
dbt test

# Generate documentation
dbt docs generate
dbt docs serve
```

### Docker Commands

```bash
# View service status
docker compose ps

# View logs (all services)
docker compose logs -f

# View logs (specific service)
docker compose logs -f parquet-loader
docker compose logs -f dbt-models

# Restart specific service
docker compose up parquet-loader

# Restart with rebuild
docker compose up --build dbt-models

# Clean restart (deletes all data)
docker compose down -v
docker compose up -d

# Run service interactively
docker compose run --rm dbt-models bash
docker compose run --rm parquet-loader python load_data_generic.py
```

---

## 🔧 Troubleshooting

### Common Issues

#### 1. parquet-loader Fails

**Symptoms:** Service exits with error

**Solutions:**
```bash
# Check PostgreSQL is healthy
docker compose ps postgres

# View loader logs
docker compose logs parquet-loader

# Run manually for debugging
docker compose run --rm parquet-loader bash
python load_data_generic.py
```

#### 2. dbt-models Fails

**Symptoms:** dbt compilation or runtime errors

**Solutions:**
```bash
# Check parquet-loader completed
docker compose logs parquet-loader | tail -20

# Check source tables exist
docker exec -it iceberg-dbt-trino trino -e "SHOW TABLES IN iceberg.raw;"

# Run dbt manually
docker compose run --rm dbt-models bash
cd /app/meterdata
dbt debug
dbt run --select staging
```

#### 3. Connection Refused

**Symptoms:** Services can't connect to each other

**Solutions:**
```bash
# Wait for healthchecks
docker compose ps

# Check service health
docker inspect iceberg-dbt-postgres --format='{{.State.Health.Status}}'

# Verify network
docker network ls
docker network inspect iceberg-dbt-etl_default
```

#### 4. Out of Memory

**Symptoms:** Services crash or become unresponsive

**Solutions:**
- Increase Docker memory: Docker Desktop → Settings → Resources → Memory (4GB+ recommended)
- Reduce parallelism: Set `DBT_THREADS: 1` in docker compose.yml

#### 5. Verification Script Fails

**Symptoms:** `./verify_docker_setup.sh` shows failed checks

**Solutions:**
```bash
# Check specific file
cat Dockerfile.parquet-loader | grep LOADER_TYPE
cat docker compose.yml | grep SOURCE_HOST
```

### Debugging Tools

```bash
# Check service logs
docker compose logs <service-name>

# Inspect service configuration
docker compose config

# Check container resources
docker stats

# Access PostgreSQL
docker exec -it iceberg-dbt-postgres psql -U dbt_user -d iceberg_dbt

# Access Trino CLI
docker exec -it iceberg-dbt-trino trino

# Access MinIO Console
open http://localhost:9001
# Login: minio_user / minio_password

# List S3 buckets/objects
docker exec -it iceberg-dbt-minio mc ls /data
```

---

## 👨‍💻 Development

### Adding New Tables

1. **Update source schema** in `init-scripts/01-schema.sql`
2. **Add test data** in `init-scripts/02-test-data.sql`
3. **Update loader** in `load_data_generic.py` or `table_config.example.json`
4. **Update dbt sources** in `models/raw/sources.yml`
5. **Update staging** in `models/staging/stg_meter_data.sql`
6. **Create Data Vault entities** in `models/raw_vault/`

### Adding New Data Sources

Implement the `DataSourceInterface`:

```python
class OracleDataSource(DataSourceInterface):
    def connect(self):
        # Implement Oracle connection via DuckDB
        pass

    def disconnect(self):
        pass

    def get_scanner_extension(self):
        return "oracle"

    def get_connection_string(self):
        return f"..."
```

### Running Tests

```bash
# dbt tests
cd meterdata
dbt test

# Python tests (if implemented)
pytest tests/

# Integration test
docker compose down -v
docker compose up -d
docker compose logs -f
```

### Performance Optimization

- **Incremental loading**: Only processes new data based on watermarks
- **File deduplication**: Prevents re-registering existing files
- **Partitioned satellites**: `sat_asset_measurements` partitioned by day
- **Business vault views**: Zero storage, computed on-the-fly
- **Iceberg features**: File pruning, metadata caching, data skipping

---

## 📚 References

### Documentation
- [Data Vault 2.0 Standards](https://datavaultalliance.com/)
- [AutomateDV Documentation](https://automate-dv.readthedocs.io/)
- [Apache Iceberg Documentation](https://iceberg.apache.org/)
- [dbt Documentation](https://docs.getdbt.com/)
- [Trino Documentation](https://trino.io/docs/)
- [DuckDB Documentation](https://duckdb.org/docs/)

### Tools & Libraries
- [PyIceberg](https://py.iceberg.apache.org/) - Python client for Iceberg
- [dbt-trino](https://github.com/starburstdata/dbt-trino) - dbt adapter for Trino
- [dbt-utils](https://github.com/dbt-labs/dbt-utils) - Utility macros
- [AutomateDV](https://github.com/Datavault-UK/automate-dv) - Data Vault automation

### Project Files
- **README.md** - This file - complete documentation
- **verify_docker_setup.sh** - Automated verification (44 checks)
- **load_data_generic.py** - Generic loader framework (650 lines)
- **table_config.example.json** - Example table configuration

---

## 🎯 Production Considerations

### Security
- [ ] Change default credentials in `docker compose.yml`
- [ ] Use secrets management (Vault, AWS Secrets Manager)
- [ ] Enable SSL/TLS for MinIO and Trino
- [ ] Implement network isolation
- [ ] Set up proper authentication and authorization

### Monitoring
- [ ] Add health checks to all services
- [ ] Set up log aggregation (ELK, Splunk)
- [ ] Monitor resource usage (Prometheus, Grafana)
- [ ] Alert on failures (PagerDuty, Slack)
- [ ] Track data quality metrics

### Scaling
- [ ] Use Docker Swarm or Kubernetes for orchestration
- [ ] Scale Trino workers for larger datasets
- [ ] Use external PostgreSQL for catalog
- [ ] Use production S3 instead of MinIO
- [ ] Implement data compaction and cleanup policies

---

## 📝 License

MIT License - See LICENSE file for details

---

## 🙋 Support

### Quick Commands

```bash
# Start pipeline
docker compose up -d

# View logs
docker compose logs -f

# Restart service
docker compose up parquet-loader

# Clean restart
docker compose down -v && docker compose up -d

# Connect to Trino
docker exec -it iceberg-dbt-trino trino

# Connect to PostgreSQL
docker exec -it iceberg-dbt-postgres psql -U dbt_user -d iceberg_dbt
```

### Getting Help

- **Verification fails?** Run `./verify_docker_setup.sh` and check output
- **Service fails?** Check logs with `docker compose logs <service-name>`
- **Need examples?** See the [Quick Start](#-quick-start) section above
- **Want to extend?** See `load_data_generic.py` for framework details

---

**Built with ❤️ using Data Vault 2.0 methodology**

🚀 **Ready to use!** Run `docker compose up -d` to start your Data Vault 2.0 pipeline.
