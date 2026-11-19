# Databricks notebook source
# DBTITLE 1,s
# MAGIC %sql
# MAGIC create schema if not exists bronze comment 'schema'

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists bronze.order_raw deep clone samples.tpch.orders;
# MAGIC create table if not exists bronze.customer_raw deep clone samples.tpch.customer;

# COMMAND ----------

# MAGIC %sql
# MAGIC select *from bronze.customer_raw

# COMMAND ----------

# DBTITLE 1,Add increment data
# MAGIC %sql
# MAGIC insert into bronze.order_raw 
# MAGIC select * from samples.tpch.orders limit 10000;

# COMMAND ----------

# MAGIC %sql
# MAGIC create volume if not exists etl.landing comment "volumne for source files"

# COMMAND ----------

# MAGIC %sql
# MAGIC select *from bronze.order_raw with limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 1) Make sure the catalog exists
# MAGIC CREATE CATALOG IF NOT EXISTS etl;
# MAGIC
# MAGIC -- (optional) choose storage location if you use external/managed per your policy
# MAGIC -- CREATE CATALOG IF NOT EXISTS etl MANAGED LOCATION 'abfss://<container>@<account>.dfs.core.windows.net/uc/etl/';
# MAGIC
# MAGIC -- 2) Make sure the schema exists
# MAGIC CREATE SCHEMA IF NOT EXISTS etl.landing;
# MAGIC
# MAGIC -- 3) Create the volume (you must include the volume name!)
# MAGIC CREATE VOLUME IF NOT EXISTS etl.landing.files
# MAGIC COMMENT 'volume for source files';
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW VOLUMES IN etl.landing;
# MAGIC

# COMMAND ----------

dbutils.fs.mkdirs('/Volumes/etl/landing/files')
dbutils.fs.mkdirs('/Volumes/etl/landing/autoloader/schemas')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS etl.landing.autoloader
# MAGIC COMMENT 'Volume for Auto Loader checkpoints & configs';

# COMMAND ----------

dbutils.fs.mkdirs('/Volumes/etl/landing/files')                       # base
dbutils.fs.mkdirs("/Volumes/etl/landing/autoloader/schemas")


# COMMAND ----------

