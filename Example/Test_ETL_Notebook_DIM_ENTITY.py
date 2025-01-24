# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS `bronze_layer`;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS bronze_layer.brnz_entity (
# MAGIC entity_name_pk STRING,
# MAGIC created_at_ts TIMESTAMP,
# MAGIC modified_at_ts TIMESTAMP,
# MAGIC data_source STRING,
# MAGIC input_file_name STRING);
# MAGIC
# MAGIC DELETE FROM bronze_layer.brnz_entity;
# MAGIC
# MAGIC INSERT INTO bronze_layer.brnz_entity
# MAGIC SELECT  'Deparment of Community Development', current_timestamp(), current_timestamp(),'hardcoded','harcoded'
# MAGIC UNION ALL
# MAGIC SELECT 'Deparment of Culture and Tourism', current_timestamp(), current_timestamp(),'hardcoded','harcoded'
# MAGIC UNION ALL
# MAGIC SELECT 'Deparment of Economic Development', current_timestamp(), current_timestamp(),'hardcoded','harcoded';
# MAGIC
# MAGIC SELECT * FROM bronze_layer.brnz_entity;
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS `silver_layer`;
# MAGIC
# MAGIC DROP TABLE silver_layer.slvr_entity;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS silver_layer.slvr_entity (
# MAGIC integration_id STRING,
# MAGIC entity_key LONG GENERATED ALWAYS AS IDENTITY,
# MAGIC entity_name STRING,
# MAGIC created_at_ts TIMESTAMP,
# MAGIC modified_at_ts TIMESTAMP,
# MAGIC data_source STRING);
# MAGIC
# MAGIC DELETE FROM silver_layer.slvr_entity;
# MAGIC
# MAGIC INSERT INTO  silver_layer.slvr_entity (integration_id,entity_name, created_at_ts,modified_at_ts,data_source)
# MAGIC WITH GROUND AS (
# MAGIC SELECT  'Unknown' AS entity_name_pk,
# MAGIC TO_TIMESTAMP('1900-01-01') AS created_at_ts,
# MAGIC TO_TIMESTAMP('1900-01-01') AS modified_at_ts,
# MAGIC 'hardcoded' as data_source
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC entity_name_pk,
# MAGIC created_at_ts,
# MAGIC modified_at_ts,
# MAGIC data_source
# MAGIC FROM bronze_layer.brnz_entity
# MAGIC )
# MAGIC SELECT 
# MAGIC NULL,
# MAGIC entity_name_pk,
# MAGIC created_at_ts,
# MAGIC modified_at_ts,
# MAGIC data_source
# MAGIC FROM GROUND
# MAGIC ;
# MAGIC
# MAGIC UPDATE silver_layer.slvr_entity
# MAGIC SET integration_id = entity_key;
# MAGIC
# MAGIC SELECT * FROM silver_layer.slvr_entity;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS `bronze_layer`;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS bronze_layer.brnz_entity_delta (
# MAGIC entity_name_pk STRING,
# MAGIC created_at_ts TIMESTAMP,
# MAGIC modified_at_ts TIMESTAMP,
# MAGIC data_source STRING,
# MAGIC input_file_name STRING);
# MAGIC
# MAGIC DELETE FROM bronze_layer.brnz_entity_delta;
# MAGIC
# MAGIC INSERT INTO bronze_layer.brnz_entity_delta
# MAGIC SELECT  'Deparment of Community Development', current_timestamp(), current_timestamp(),'hardcoded','harcoded'
# MAGIC UNION ALL
# MAGIC SELECT 'Deparment of Culture and Tourism', current_timestamp(), current_timestamp(),'hardcoded','harcoded'
# MAGIC UNION ALL
# MAGIC SELECT 'Deparment of Economic Development', current_timestamp(), current_timestamp(),'TEST','harcoded'
# MAGIC /*UNION ALL 
# MAGIC SELECT 'Deparment of Energy', current_timestamp(), current_timestamp(), 'hardcoded', 'hardcoded'*/
# MAGIC ;
# MAGIC
# MAGIC SELECT * FROM bronze_layer.brnz_entity_delta;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC MERGE INTO silver_layer.slvr_entity target
# MAGIC USING (
# MAGIC SELECT 
# MAGIC -1 AS integration_id,
# MAGIC -1 AS entity_key,
# MAGIC 'Unknown' AS entity_name_pk,
# MAGIC TO_TIMESTAMP('1900-01-01') AS created_at_ts,
# MAGIC TO_TIMESTAMP('1900-01-01') AS modified_at_ts,
# MAGIC 'Unknown' AS data_source
# MAGIC ) source
# MAGIC ON target.entity_name = source.entity_name_pk
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (integration_id,entity_name, created_at_ts,modified_at_ts,data_source)
# MAGIC   VALUES(
# MAGIC   source.integration_id,
# MAGIC   source.entity_name_pk, 
# MAGIC   source.created_at_ts,
# MAGIC   source.modified_at_ts,
# MAGIC   source.data_source);
# MAGIC
# MAGIC MERGE INTO silver_layer.slvr_entity target
# MAGIC USING (
# MAGIC SELECT * FROM 
# MAGIC bronze_layer.brnz_entity_delta
# MAGIC ) source
# MAGIC ON target.entity_name = source.entity_name_pk
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET 
# MAGIC   modified_at_ts = current_timestamp(),
# MAGIC   data_source = source.data_source
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (integration_id,entity_name, created_at_ts,modified_at_ts,data_source)
# MAGIC   VALUES(NULL,
# MAGIC   entity_name_pk, 
# MAGIC   current_timestamp(),
# MAGIC   current_timestamp(),
# MAGIC   source.data_source);
# MAGIC
# MAGIC UPDATE silver_layer.slvr_entity
# MAGIC SET integration_id = entity_key;
# MAGIC
# MAGIC DELETE FROM silver_layer.slvr_entity
# MAGIC WHERE integration_id IN (
# MAGIC SELECT SLVR.integration_id
# MAGIC FROM silver_layer.slvr_entity AS SLVR
# MAGIC LEFT JOIN bronze_layer.brnz_entity_delta BRNZ
# MAGIC ON SLVR.entity_name = BRNZ.entity_name_pk
# MAGIC WHERE BRNZ.entity_name_pk IS NULL
# MAGIC AND SLVR.entity_name != 'Unknown'
# MAGIC );
# MAGIC
# MAGIC SELECT * FROM silver_layer.slvr_entity;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS `gold_layer`;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS gold_layer.gld_dim_entity (
# MAGIC integration_id STRING,
# MAGIC entity_key LONG,
# MAGIC entity_name STRING,
# MAGIC created_at_ts TIMESTAMP,
# MAGIC modified_at_ts TIMESTAMP,
# MAGIC start_ts TIMESTAMP,
# MAGIC end_ts TIMESTAMP,
# MAGIC current_flag BOOLEAN,
# MAGIC data_source STRING);
# MAGIC
# MAGIC --DELETE FROM gold_layer.gld_dim_entity;
# MAGIC
# MAGIC MERGE INTO gold_layer.gld_dim_entity target
# MAGIC USING (
# MAGIC SELECT * FROM 
# MAGIC silver_layer.slvr_entity
# MAGIC ) source
# MAGIC ON target.integration_id = source.integration_id
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET 
# MAGIC   entity_name = source.entity_name
# MAGIC   modified_at_ts = current_timestamp(),
# MAGIC   data_source = source.data_source
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (integration_id,entity_key, entity_name,created_at_ts,modified_at_ts, start_ts, end_ts, current_flag,data_source)
# MAGIC   VALUES(
# MAGIC   source.integration_id,
# MAGIC   source.entity_key,
# MAGIC   source.entity_name, 
# MAGIC   current_timestamp(),
# MAGIC   current_timestamp(),
# MAGIC   current_timestamp(),
# MAGIC   NULL,
# MAGIC   1,
# MAGIC   source.data_source);
# MAGIC
# MAGIC UPDATE gold_layer.gld_dim_entity
# MAGIC SET 
# MAGIC modified_at_ts = current_timestamp(),
# MAGIC end_ts = current_timestamp(),
# MAGIC current_flag = 0
# MAGIC WHERE integration_id IN (
# MAGIC SELECT GLD.integration_id
# MAGIC FROM gold_layer.gld_dim_entity AS GLD
# MAGIC LEFT JOIN silver_layer.slvr_entity SLVR
# MAGIC ON GLD.integration_id = SLVR.integration_id
# MAGIC WHERE SLVR.integration_id IS NULL
# MAGIC );
# MAGIC
# MAGIC SELECT * FROM gold_layer.gld_dim_entity;
# MAGIC
# MAGIC
