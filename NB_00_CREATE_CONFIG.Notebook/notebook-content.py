# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "2c8ab76d-63a1-4806-a8cc-535bba0b5555",
# META       "default_lakehouse_name": "LH_API_INGESTION",
# META       "default_lakehouse_workspace_id": "ea224e1d-5a3b-4811-b9c3-7d78c27f52c9",
# META       "known_lakehouses": [
# META         {
# META           "id": "2c8ab76d-63a1-4806-a8cc-535bba0b5555"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
from pyspark.sql import functions as F
from pyspark.sql.types import *

print("Initializing API Ingestion Framework")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

api_source_schema = StructType([
    StructField("source_id", IntegerType(), False),
    StructField("source_name", StringType(), False),
    StructField("base_url", StringType(), False),
    StructField("endpoint", StringType(), False),
    StructField("response_data_key", StringType(), False),
    StructField("pagination_type", StringType(), False),
    StructField("page_size", IntegerType(), False),
    StructField("enabled", BooleanType(), False),
    StructField("auth_type", StringType(), False),
    StructField("load_type", StringType(), False)
])

api_source_rows = [
    (1, "products", "https://dummyjson.com",
     "/products", "products", "OFFSET", 30, True, "NONE", "FULL"),

    (2, "carts", "https://dummyjson.com",
     "/carts", "carts", "OFFSET", 30, True, "NONE", "FULL"),

    (3, "users", "https://dummyjson.com",
     "/users", "users", "OFFSET", 30, True, "NONE", "FULL")
]

cfg_api_source = spark.createDataFrame(
    api_source_rows,
    api_source_schema
)

cfg_api_source.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("cfg_api_source")

display(spark.table("cfg_api_source"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

entity_schema = StructType([
    StructField("source_name", StringType(), False),
    StructField("target_table", StringType(), False),
    StructField("transform_type", StringType(), False),
    StructField("primary_key", StringType(), False),
    StructField("enabled", BooleanType(), False)
])

entity_rows = [
    ("products", "silver_products", "PRODUCT", "product_id", True),
    ("carts", "silver_carts", "CART", "cart_id", True),
    ("users", "silver_users", "USER", "user_id", True)
]

cfg_entity = spark.createDataFrame(
    entity_rows,
    entity_schema
)

cfg_entity.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("cfg_entity")

display(spark.table("cfg_entity"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

audit_schema = StructType([
    StructField("run_id", StringType(), False),
    StructField("source_name", StringType(), False),
    StructField("status", StringType(), False),
    StructField("records_processed", LongType(), True),
    StructField("pages_processed", IntegerType(), True),
    StructField("bronze_path", StringType(), True),
    StructField("error_message", StringType(), True),
    StructField("started_at", StringType(), True),
    StructField("completed_at", StringType(), True)
])

audit_df = spark.createDataFrame([], audit_schema)

audit_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("etl_run_audit")

print("ETL audit table created successfully")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("API SOURCES")

display(
    spark.sql("""
        SELECT *
        FROM cfg_api_source
    """)
)

print("ENTITY CONFIGURATION")

display(
    spark.sql("""
        SELECT *
        FROM cfg_entity
    """)
)

print("AUDIT TABLE")

display(
    spark.sql("""
        SELECT *
        FROM etl_run_audit
    """)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
