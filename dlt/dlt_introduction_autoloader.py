# Databricks notebook source
import dlt

# COMMAND ----------

#create streaming tables for orders
@dlt.table(
    table_properties = {"quality": "bronze"},
    comment = "Order Bronze Table   "
)
def order_bronze():
    df = spark.readStream.table("bronze.order_raw")
    return df


# COMMAND ----------

#create streaming tables for orders using Autoloader
@dlt.table(
    table_properties = {"quality": "bronze"},
    comment = "Order Autoloader",
    name = "orders_autoloader_bronze"
)
def func():
    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.schemaHints", 
                "o_orderkey long, o_custkey long, o_orderstatus string, "
                "o_totalprice decimal(18,2), o_orderdate date, "
                "o_orderpriority string, o_clerk string, "
                "o_shippriority integer, o_comment string")
        .option("cloudFiles.schemaLocation", "/Volumes/etl/landing/autoloader/schemas/1/")
        .option("cloudFiles.format", "csv")
        .option("pathGlobFilter", "*.csv")
        .option("cloudFiles.schemaEvolutionMode", "none")
        .load("/Volumes/etl/landing/files/")
    )
    return df


# COMMAND ----------

import dlt
from pyspark.sql import functions as F

# Example: two upstream bronze streams must already exist in the pipeline:
#   - order_bronze
#   - orders_autoloader_bronze

@dlt.table(
    name="orders_union_bronze",
    comment="Union of order streams from two bronze sources",
    table_properties={
        "quality": "bronze",
        "delta.appendOnly": "true"  # optional: enforce append-only
    }
)
def orders_union_bronze():
    df1 = dlt.read_stream("order_bronze")
    df2 = dlt.read_stream("orders_autoloader_bronze")
    # union in append mode
    return df1.unionByName(df2, allowMissingColumns=True)


# COMMAND ----------

#create materilized view for customer

@dlt.table(
    table_properties = {"quality": "bronze"},
    comment = "Customer Bronze Table   "
)
def customer_bronze():
    df = spark.read.table("bronze.customer_raw")
    return df

# COMMAND ----------

#create a view to join order with customer
@dlt.view(
     comment = "Joined View"
)
def joined_vw():
    df_c     = spark.read.table("LIVE.customer_bronze")
    df_o     = spark.read.table("LIVE.orders_union_bronze")
    df_joined = df_o.join(df_c,how= "left_outer", on = df_c.c_custkey == df_o.o_custkey)
    return df_joined

# COMMAND ----------

#create materilized view add a new column
from pyspark.sql.functions import current_timestamp,count

@dlt.table(
    table_properties = {"quality": "silver"},
    comment = "joined table",
    name = "joined_silver")
def joined_silver():
    df = spark.read.table("LIVE.joined_vw").withColumn('insert_date',current_timestamp())
    return df

# COMMAND ----------

#create aggregation
@dlt.table(
    table_properties = {"quality": "gold"},
    comment = "order_aggregate_table")
def order_aggregate_gold():
    df = spark.read.table("LIVE.joined_silver")
    df_final = df.groupBy('c_mktsegment').agg(count("c_custkey").alias("sum_orders")).withColumn('insert_date',current_timestamp())
    return df_final