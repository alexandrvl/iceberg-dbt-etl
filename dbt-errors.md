2025-10-03T12:04:01.539715390Z ==========================================
2025-10-03T12:04:01.539729887Z Building Data Vault 2.0 Models
2025-10-03T12:04:01.539731751Z ==========================================
2025-10-03T12:04:01.539733073Z 
2025-10-03T12:04:01.539734225Z Step 1: Installing dbt dependencies (AutomateDV + dbt-utils)
2025-10-03T12:04:02.392678542Z 12:04:02  Running with dbt=1.10.7
2025-10-03T12:04:02.451144873Z 12:04:02  Warning: No packages were found in packages.yml
2025-10-03T12:04:02.451438002Z 12:04:02  Warning: No packages were found in packages.yml
2025-10-03T12:04:03.370056159Z 
2025-10-03T12:04:03.370075525Z Step 2: Building staging layer
2025-10-03T12:04:03.370078190Z   - Joins source tables (customers, accounts, assets, readings)
2025-10-03T12:04:03.370080224Z   - Generates hash keys for hubs
2025-10-03T12:04:03.370081957Z   - Generates hash diffs for satellites
2025-10-03T12:04:04.101771920Z 12:04:04  Running with dbt=1.10.7
2025-10-03T12:04:04.121517078Z INFO:trino.auth:keyring module not found. OAuth2 token will not be stored in keyring.
2025-10-03T12:04:04.122421795Z INFO:trino.auth:keyring module not found. OAuth2 token will not be stored in keyring.
2025-10-03T12:04:04.215043326Z 12:04:04  Registered adapter: trino=1.9.3
2025-10-03T12:04:04.316122901Z 12:04:04  Unable to do partial parsing because config vars, config profile, or config target have changed
2025-10-03T12:04:04.316354155Z 12:04:04  Unable to do partial parsing because profile has changed
2025-10-03T12:04:04.937180296Z 12:04:04  Found 2 models, 10 data tests, 1 source, 456 macros
2025-10-03T12:04:04.937592389Z 12:04:04  The selection criterion 'staging' does not match any enabled nodes
2025-10-03T12:04:04.938179100Z 12:04:04  Nothing to do. Try checking your model configs and model specification args
2025-10-03T12:04:05.878422665Z 
2025-10-03T12:04:05.878442512Z Step 3: Building raw vault (Data Vault 2.0 core)
2025-10-03T12:04:05.878444807Z   - Hubs: hub_customer, hub_customer_account, hub_asset
2025-10-03T12:04:05.878446580Z   - Links: link_customer_account, link_account_asset
2025-10-03T12:04:05.878448013Z   - Satellites: sat_customer_details, sat_customer_acc_details,
2025-10-03T12:04:05.878449706Z                 sat_asset_details, sat_asset_measurements
2025-10-03T12:04:05.878451159Z   - References: ref_meter_type, ref_reading_type, ref_status, ref_quality_code
2025-10-03T12:04:06.593032961Z 12:04:06  Running with dbt=1.10.7
2025-10-03T12:04:06.610039352Z INFO:trino.auth:keyring module not found. OAuth2 token will not be stored in keyring.
2025-10-03T12:04:06.610954248Z INFO:trino.auth:keyring module not found. OAuth2 token will not be stored in keyring.
2025-10-03T12:04:06.702728170Z 12:04:06  Registered adapter: trino=1.9.3
2025-10-03T12:04:06.874545290Z 12:04:06  Found 2 models, 10 data tests, 1 source, 456 macros
2025-10-03T12:04:06.874950760Z 12:04:06  The selection criterion 'raw_vault' does not match any enabled nodes
2025-10-03T12:04:06.875451079Z 12:04:06  Nothing to do. Try checking your model configs and model specification args
2025-10-03T12:04:07.981283472Z 
2025-10-03T12:04:07.981304050Z Step 4: Building business vault (consumption layer)
2025-10-03T12:04:07.981306675Z   - bv_customer_accounts
2025-10-03T12:04:07.981308619Z   - bv_asset_details
2025-10-03T12:04:07.981310372Z   - bv_asset_measurements
2025-10-03T12:04:07.981312135Z   - bv_customer_asset_hierarchy
2025-10-03T12:04:08.711914175Z 12:04:08  Running with dbt=1.10.7
2025-10-03T12:04:08.729704357Z INFO:trino.auth:keyring module not found. OAuth2 token will not be stored in keyring.
2025-10-03T12:04:08.730586291Z INFO:trino.auth:keyring module not found. OAuth2 token will not be stored in keyring.
2025-10-03T12:04:08.823551259Z 12:04:08  Registered adapter: trino=1.9.3
2025-10-03T12:04:08.995480563Z 12:04:08  Found 2 models, 10 data tests, 1 source, 456 macros
2025-10-03T12:04:08.995903085Z 12:04:08  The selection criterion 'business_vault' does not match any enabled nodes
2025-10-03T12:04:08.996394347Z 12:04:08  Nothing to do. Try checking your model configs and model specification args
2025-10-03T12:04:10.086106966Z 
2025-10-03T12:04:10.086129900Z Step 5: Running data quality tests
2025-10-03T12:04:10.807107310Z 12:04:10  Running with dbt=1.10.7
2025-10-03T12:04:10.823730262Z INFO:trino.auth:keyring module not found. OAuth2 token will not be stored in keyring.
2025-10-03T12:04:10.824610534Z INFO:trino.auth:keyring module not found. OAuth2 token will not be stored in keyring.
2025-10-03T12:04:10.915200258Z 12:04:10  Registered adapter: trino=1.9.3
2025-10-03T12:04:11.087632390Z 12:04:11  Found 2 models, 10 data tests, 1 source, 456 macros
2025-10-03T12:04:11.088698539Z 12:04:11  
2025-10-03T12:04:11.088843922Z 12:04:11  Concurrency: 1 threads (target='prod')
2025-10-03T12:04:11.088961553Z 12:04:11  
2025-10-03T12:04:11.119003368Z 12:04:11  [WARNING]: SSL certificate validation is disabled by default. It is legacy behavior which will be changed in future releases. It is strongly advised to enable `require_certificate_validation` flag or explicitly set `cert` configuration to `True` for security reasons. You may receive an error after that if your SSL setup is incorrect.
2025-10-03T12:04:11.119016362Z You may opt into the new behavior sooner by setting `flags.require_certificate_validation` to `True` in `dbt_project.yml`.
2025-10-03T12:04:11.119018606Z Visit https://docs.getdbt.com/reference/global-configs/behavior-changes for more information.
2025-10-03T12:04:11.838381786Z 12:04:11  1 of 10 START test not_null_meter_daily_consumption_meter_id ................... [RUN]
2025-10-03T12:04:11.984751729Z 12:04:11  1 of 10 ERROR not_null_meter_daily_consumption_meter_id ........................ [ERROR in 0.15s]
2025-10-03T12:04:11.985830072Z 12:04:11  2 of 10 START test not_null_meter_daily_consumption_reading_date ............... [RUN]
2025-10-03T12:04:12.023894757Z 12:04:12  2 of 10 ERROR not_null_meter_daily_consumption_reading_date .................... [ERROR in 0.04s]
2025-10-03T12:04:12.024761663Z 12:04:12  3 of 10 START test not_null_meter_daily_consumption_total_value ................ [RUN]
2025-10-03T12:04:12.053380920Z 12:04:12  3 of 10 ERROR not_null_meter_daily_consumption_total_value ..................... [ERROR in 0.03s]
2025-10-03T12:04:12.054174077Z 12:04:12  4 of 10 START test not_null_readings__silver_creation_date ..................... [RUN]
2025-10-03T12:04:12.088130396Z 12:04:12  4 of 10 ERROR not_null_readings__silver_creation_date .......................... [ERROR in 0.03s]
2025-10-03T12:04:12.088918504Z 12:04:12  5 of 10 START test not_null_readings__silver_creation_time ..................... [RUN]
2025-10-03T12:04:12.115474200Z 12:04:12  5 of 10 ERROR not_null_readings__silver_creation_time .......................... [ERROR in 0.03s]
2025-10-03T12:04:12.116525933Z 12:04:12  6 of 10 START test not_null_readings__silver_id ................................ [RUN]
2025-10-03T12:04:12.143518318Z 12:04:12  6 of 10 ERROR not_null_readings__silver_id ..................................... [ERROR in 0.03s]
2025-10-03T12:04:12.144510358Z 12:04:12  7 of 10 START test not_null_readings__silver_interval_start .................... [RUN]
2025-10-03T12:04:12.171835678Z 12:04:12  7 of 10 ERROR not_null_readings__silver_interval_start ......................... [ERROR in 0.03s]
2025-10-03T12:04:12.172648653Z 12:04:12  8 of 10 START test not_null_readings__silver_meter_id .......................... [RUN]
2025-10-03T12:04:12.199046743Z 12:04:12  8 of 10 ERROR not_null_readings__silver_meter_id ............................... [ERROR in 0.03s]
2025-10-03T12:04:12.199891788Z 12:04:12  9 of 10 START test not_null_readings__silver_reading_value ..................... [RUN]
2025-10-03T12:04:12.224228040Z 12:04:12  9 of 10 ERROR not_null_readings__silver_reading_value .......................... [ERROR in 0.02s]
2025-10-03T12:04:12.225032118Z 12:04:12  10 of 10 START test unique_readings__silver_id ................................. [RUN]
2025-10-03T12:04:12.252509593Z 12:04:12  10 of 10 ERROR unique_readings__silver_id ...................................... [ERROR in 0.03s]
2025-10-03T12:04:12.254627727Z 12:04:12  
2025-10-03T12:04:12.254845796Z 12:04:12  Finished running 10 data tests in 0 hours 0 minutes and 1.17 seconds (1.17s).
2025-10-03T12:04:12.271640301Z 12:04:12  
2025-10-03T12:04:12.271800972Z 12:04:12  Completed with 10 errors, 0 partial successes, and 0 warnings:
2025-10-03T12:04:12.271903504Z 12:04:12  
2025-10-03T12:04:12.272046422Z 12:04:12  Failure in test not_null_meter_daily_consumption_meter_id (models/gold/schema.yml)
2025-10-03T12:04:12.272186746Z 12:04:12    Database Error in test not_null_meter_daily_consumption_meter_id (models/gold/schema.yml)
2025-10-03T12:04:12.272191014Z   TrinoUserError(type=USER_ERROR, name=SCHEMA_NOT_FOUND, message="line 17:6: Schema 'raw_vault_gold' does not exist", query_id=20251003_120411_00002_5pzvw)
2025-10-03T12:04:12.272193378Z   compiled code at target/run/meterdata/models/gold/schema.yml/not_null_meter_daily_consumption_meter_id.sql
2025-10-03T12:04:12.272278378Z 12:04:12  
2025-10-03T12:04:12.272401729Z 12:04:12    compiled code at target/compiled/meterdata/models/gold/schema.yml/not_null_meter_daily_consumption_meter_id.sql
2025-10-03T12:04:12.272497098Z 12:04:12  
2025-10-03T12:04:12.272626981Z 12:04:12  Failure in test not_null_meter_daily_consumption_reading_date (models/gold/schema.yml)
2025-10-03T12:04:12.272761474Z 12:04:12    Database Error in test not_null_meter_daily_consumption_reading_date (models/gold/schema.yml)
2025-10-03T12:04:12.272764570Z   TrinoUserError(type=USER_ERROR, name=SCHEMA_NOT_FOUND, message="line 17:6: Schema 'raw_vault_gold' does not exist", query_id=20251003_120412_00003_5pzvw)
2025-10-03T12:04:12.272766313Z   compiled code at target/run/meterdata/models/gold/schema.yml/not_null_meter_daily_consumption_reading_date.sql
2025-10-03T12:04:12.272848056Z 12:04:12  
2025-10-03T12:04:12.272964765Z 12:04:12    compiled code at target/compiled/meterdata/models/gold/schema.yml/not_null_meter_daily_consumption_reading_date.sql
2025-10-03T12:04:12.273055175Z 12:04:12  
2025-10-03T12:04:12.273176693Z 12:04:12  Failure in test not_null_meter_daily_consumption_total_value (models/gold/schema.yml)
2025-10-03T12:04:12.273303861Z 12:04:12    Database Error in test not_null_meter_daily_consumption_total_value (models/gold/schema.yml)
2025-10-03T12:04:12.273306336Z   TrinoUserError(type=USER_ERROR, name=SCHEMA_NOT_FOUND, message="line 17:6: Schema 'raw_vault_gold' does not exist", query_id=20251003_120412_00004_5pzvw)
2025-10-03T12:04:12.273308029Z   compiled code at target/run/meterdata/models/gold/schema.yml/not_null_meter_daily_consumption_total_value.sql
2025-10-03T12:04:12.273397326Z 12:04:12  
2025-10-03T12:04:12.273510929Z 12:04:12    compiled code at target/compiled/meterdata/models/gold/schema.yml/not_null_meter_daily_consumption_total_value.sql
2025-10-03T12:04:12.273600838Z 12:04:12  
2025-10-03T12:04:12.273731573Z 12:04:12  Failure in test not_null_readings__silver_creation_date (models/silver/schema.yml)
2025-10-03T12:04:12.273862238Z 12:04:12    Database Error in test not_null_readings__silver_creation_date (models/silver/schema.yml)
2025-10-03T12:04:12.273867158Z   TrinoUserError(type=USER_ERROR, name=SCHEMA_NOT_FOUND, message="line 17:6: Schema 'raw_vault_silver' does not exist", query_id=20251003_120412_00005_5pzvw)
2025-10-03T12:04:12.273868901Z   compiled code at target/run/meterdata/models/silver/schema.yml/not_null_readings__silver_creation_date.sql
2025-10-03T12:04:12.273948991Z 12:04:12  
2025-10-03T12:04:12.274062153Z 12:04:12    compiled code at target/compiled/meterdata/models/silver/schema.yml/not_null_readings__silver_creation_date.sql
2025-10-03T12:04:12.274150770Z 12:04:12  
2025-10-03T12:04:12.274268551Z 12:04:12  Failure in test not_null_readings__silver_creation_time (models/silver/schema.yml)
2025-10-03T12:04:12.274393685Z 12:04:12    Database Error in test not_null_readings__silver_creation_time (models/silver/schema.yml)
2025-10-03T12:04:12.274396080Z   TrinoUserError(type=USER_ERROR, name=SCHEMA_NOT_FOUND, message="line 17:6: Schema 'raw_vault_silver' does not exist", query_id=20251003_120412_00006_5pzvw)
2025-10-03T12:04:12.274397753Z   compiled code at target/run/meterdata/models/silver/schema.yml/not_null_readings__silver_creation_time.sql
2025-10-03T12:04:12.274480037Z 12:04:12  
2025-10-03T12:04:12.274603218Z 12:04:12    compiled code at target/compiled/meterdata/models/silver/schema.yml/not_null_readings__silver_creation_time.sql
2025-10-03T12:04:12.274692746Z 12:04:12  
2025-10-03T12:04:12.274814004Z 12:04:12  Failure in test not_null_readings__silver_id (models/silver/schema.yml)
2025-10-03T12:04:12.274940892Z 12:04:12    Database Error in test not_null_readings__silver_id (models/silver/schema.yml)
2025-10-03T12:04:12.274943316Z   TrinoUserError(type=USER_ERROR, name=SCHEMA_NOT_FOUND, message="line 17:6: Schema 'raw_vault_silver' does not exist", query_id=20251003_120412_00007_5pzvw)
2025-10-03T12:04:12.274944909Z   compiled code at target/run/meterdata/models/silver/schema.yml/not_null_readings__silver_id.sql
2025-10-03T12:04:12.275030430Z 12:04:12  
2025-10-03T12:04:12.275142991Z 12:04:12    compiled code at target/compiled/meterdata/models/silver/schema.yml/not_null_readings__silver_id.sql
2025-10-03T12:04:12.275234062Z 12:04:12  
2025-10-03T12:04:12.275353226Z 12:04:12  Failure in test not_null_readings__silver_interval_start (models/silver/schema.yml)
2025-10-03T12:04:12.275478310Z 12:04:12    Database Error in test not_null_readings__silver_interval_start (models/silver/schema.yml)
2025-10-03T12:04:12.275480765Z   TrinoUserError(type=USER_ERROR, name=SCHEMA_NOT_FOUND, message="line 17:6: Schema 'raw_vault_silver' does not exist", query_id=20251003_120412_00008_5pzvw)
2025-10-03T12:04:12.275482278Z   compiled code at target/run/meterdata/models/silver/schema.yml/not_null_readings__silver_interval_start.sql
2025-10-03T12:04:12.275564672Z 12:04:12  
2025-10-03T12:04:12.275683896Z 12:04:12    compiled code at target/compiled/meterdata/models/silver/schema.yml/not_null_readings__silver_interval_start.sql
2025-10-03T12:04:12.275775277Z 12:04:12  
2025-10-03T12:04:12.275890834Z 12:04:12  Failure in test not_null_readings__silver_meter_id (models/silver/schema.yml)
2025-10-03T12:04:12.276010338Z 12:04:12    Database Error in test not_null_readings__silver_meter_id (models/silver/schema.yml)
2025-10-03T12:04:12.276012763Z   TrinoUserError(type=USER_ERROR, name=SCHEMA_NOT_FOUND, message="line 17:6: Schema 'raw_vault_silver' does not exist", query_id=20251003_120412_00009_5pzvw)
2025-10-03T12:04:12.276014336Z   compiled code at target/run/meterdata/models/silver/schema.yml/not_null_readings__silver_meter_id.sql
2025-10-03T12:04:12.276100337Z 12:04:12  
2025-10-03T12:04:12.276213760Z 12:04:12    compiled code at target/compiled/meterdata/models/silver/schema.yml/not_null_readings__silver_meter_id.sql
2025-10-03T12:04:12.276301174Z 12:04:12  
2025-10-03T12:04:12.276415298Z 12:04:12  Failure in test not_null_readings__silver_reading_value (models/silver/schema.yml)
2025-10-03T12:04:12.276539080Z 12:04:12    Database Error in test not_null_readings__silver_reading_value (models/silver/schema.yml)
2025-10-03T12:04:12.276541545Z   TrinoUserError(type=USER_ERROR, name=SCHEMA_NOT_FOUND, message="line 17:6: Schema 'raw_vault_silver' does not exist", query_id=20251003_120412_00010_5pzvw)
2025-10-03T12:04:12.276543088Z   compiled code at target/run/meterdata/models/silver/schema.yml/not_null_readings__silver_reading_value.sql
2025-10-03T12:04:12.276626384Z 12:04:12  
2025-10-03T12:04:12.276740828Z 12:04:12    compiled code at target/compiled/meterdata/models/silver/schema.yml/not_null_readings__silver_reading_value.sql
2025-10-03T12:04:12.276825587Z 12:04:12  
2025-10-03T12:04:12.276938529Z 12:04:12  Failure in test unique_readings__silver_id (models/silver/schema.yml)
2025-10-03T12:04:12.277055930Z 12:04:12    Database Error in test unique_readings__silver_id (models/silver/schema.yml)
2025-10-03T12:04:12.277058324Z   TrinoUserError(type=USER_ERROR, name=SCHEMA_NOT_FOUND, message="line 18:6: Schema 'raw_vault_silver' does not exist", query_id=20251003_120412_00011_5pzvw)
2025-10-03T12:04:12.277059927Z   compiled code at target/run/meterdata/models/silver/schema.yml/unique_readings__silver_id.sql
2025-10-03T12:04:12.277145327Z 12:04:12  
2025-10-03T12:04:12.277249372Z 12:04:12    compiled code at target/compiled/meterdata/models/silver/schema.yml/unique_readings__silver_id.sql
2025-10-03T12:04:12.277340704Z 12:04:12  
2025-10-03T12:04:12.277453165Z 12:04:12  Done. PASS=0 WARN=0 ERROR=10 SKIP=0 NO-OP=0 TOTAL=10
2025-10-03T12:04:12.982608409Z ⚠️  Some tests failed - check logs
2025-10-03T12:04:12.982636923Z 
2025-10-03T12:04:12.982640379Z ==========================================
2025-10-03T12:04:12.982652783Z ✅ Data Vault 2.0 Build Complete!
2025-10-03T12:04:12.982654596Z ==========================================
2025-10-03T12:04:12.982656319Z 
2025-10-03T12:04:12.982657952Z 📊 Available schemas:
2025-10-03T12:04:12.982659585Z   • iceberg.staging - Prepared source with hash keys
2025-10-03T12:04:12.982661269Z   • iceberg.raw_vault - Hubs, Links, Satellites, References
2025-10-03T12:04:12.982662942Z   • iceberg.business_vault - Denormalized views
2025-10-03T12:04:12.982664625Z 
2025-10-03T12:04:12.982666098Z 🔍 Query examples:
2025-10-03T12:04:12.982667651Z   docker exec -it iceberg-dbt-trino trino
2025-10-03T12:04:12.982669644Z   SELECT * FROM iceberg.business_vault.bv_customer_asset_hierarchy;
2025-10-03T12:04:12.982671708Z   SELECT * FROM iceberg.raw_vault.sat_asset_measurements LIMIT 10;
2025-10-03T12:04:12.982673391Z 
2025-10-03T12:04:12.982674834Z 📚 Full documentation: See README.md
