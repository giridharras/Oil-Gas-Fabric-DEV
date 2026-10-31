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
from delta.tables import DeltaTable

from datetime import datetime, timezone

print("Bronze to Silver framework initialized")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# None = Process all enabled entities
# "products" = Process only Products
# "carts" = Process only Carts
# "users" = Process only Users

p_source_name = None

print("Source Filter:", p_source_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Validate required tables

required_tables = [
    "cfg_api_source",
    "cfg_entity",
    "etl_run_audit"
]

for table_name in required_tables:

    if not spark.catalog.tableExists(table_name):

        raise Exception(
            f"Required table missing: {table_name}"
        )


# Read API configuration
api_config_df = (
    spark.table("cfg_api_source")
    .filter(F.col("enabled") == True)
)

# Read entity configuration
entity_config_df = (
    spark.table("cfg_entity")
    .filter(F.col("enabled") == True)
)

# Optional source filter
if p_source_name:

    api_config_df = api_config_df.filter(
        F.col("source_name") == p_source_name
    )

    entity_config_df = entity_config_df.filter(
        F.col("source_name") == p_source_name
    )


# Convert configurations into dictionaries
api_config = {
    row["source_name"]: row.asDict()
    for row in api_config_df.collect()
}

entity_config = [
    row.asDict()
    for row in entity_config_df.collect()
]


print("Enabled API sources:", list(api_config.keys()))

print("Enabled entities:", len(entity_config))

display(entity_config_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

source_name = entity["source_name"]
response_data_key = api_config[source_name]["response_data_key"]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

source_name = orders
target_table = silver_orders
transform_type = ORDER
primary_key = order_id

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_latest_successful_run(source_name):

    audit_df = spark.table("etl_run_audit")

    latest_run = (
        audit_df
        .filter(
            (F.col("source_name") == source_name) &
            (F.col("status") == "SUCCESS") &
            F.col("bronze_path").isNotNull()
        )
        .orderBy(
            F.col("completed_at").desc()
        )
        .limit(1)
    )

    result = latest_run.collect()

    if not result:

        raise Exception(
            f"No successful Bronze execution found "
            f"for {source_name}"
        )

    return result[0].asDict()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

latest_products_run = get_latest_successful_run(
    "products"
)

print("Run ID:", latest_products_run["run_id"])

print("Bronze Path:", latest_products_run["bronze_path"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def read_bronze_data(source_name):

    # Get latest successful API execution
    run_details = get_latest_successful_run(
        source_name
    )

    bronze_path = run_details["bronze_path"]

    # Read exact run folder
    raw_df = (
        spark.read
        .option("multiLine", True)
        .json(bronze_path)
    )

    print("Source:", source_name)

    print("Run ID:", run_details["run_id"])

    print("Bronze Path:", bronze_path)

    print("JSON Columns:", raw_df.columns)

    return raw_df, run_details

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

products_raw, products_run = read_bronze_data(
    "products"
)

display(products_raw)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def validate_silver_data(
    df,
    key_columns,
    entity_name
):

    print(f"Validating: {entity_name}")

    # Check record count
    total_records = df.count()

    if total_records == 0:

        raise Exception(
            f"{entity_name}: No records found"
        )

    # Check NULL primary keys
    null_condition = None

    for column in key_columns:

        condition = F.col(column).isNull()

        null_condition = (
            condition
            if null_condition is None
            else null_condition | condition
        )

    null_count = (
        df.filter(null_condition)
        .count()
    )

    if null_count > 0:

        raise Exception(
            f"{entity_name}: "
            f"{null_count} records have NULL keys"
        )

    # Check duplicate keys
    duplicate_count = (
        df.groupBy(*key_columns)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    if duplicate_count > 0:

        raise Exception(
            f"{entity_name}: "
            f"{duplicate_count} duplicate key combinations found"
        )

    print("Total Records:", total_records)

    print("NULL Keys:", null_count)

    print("Duplicate Keys:", duplicate_count)

    print("Validation Successful")

    return total_records

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def upsert_delta_table(
    source_df,
    target_table,
    key_columns
):

    # Check whether target exists
    if spark.catalog.tableExists(target_table):

        print(
            f"MERGE into existing table: {target_table}"
        )

        delta_table = DeltaTable.forName(
            spark,
            target_table
        )

        # Build dynamic join condition
        merge_condition = " AND ".join(
            [
                f"target.`{key}` = source.`{key}`"
                for key in key_columns
            ]
        )

        (
            delta_table.alias("target")
            .merge(
                source_df.alias("source"),
                merge_condition
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

    else:

        print(
            f"Creating new Delta table: {target_table}"
        )

        (
            source_df.write
            .format("delta")
            .mode("errorifexists")
            .saveAsTable(target_table)
        )

    print(
        f"Successfully loaded: {target_table}"
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def transform_products(
    raw_df,
    run_id
):

    products_df = raw_df.select(
        F.explode("products").alias("product")
    )

    silver_df = products_df.select(

        F.col("product.id")
            .cast("int")
            .alias("product_id"),

        F.col("product.title")
            .cast("string")
            .alias("product_name"),

        F.col("product.description")
            .cast("string")
            .alias("description"),

        F.col("product.category")
            .cast("string")
            .alias("category"),

        F.col("product.price")
            .cast("decimal(18,2)")
            .alias("price"),

        F.col("product.discountPercentage")
            .cast("double")
            .alias("discount_percentage"),

        F.col("product.rating")
            .cast("double")
            .alias("rating"),

        F.col("product.stock")
            .cast("int")
            .alias("stock"),

        F.col("product.brand")
            .cast("string")
            .alias("brand"),

        F.lit(run_id)
            .alias("source_run_id"),

        F.current_timestamp()
            .alias("processed_timestamp")
    )

    return silver_df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

products_raw, products_run = read_bronze_data(
    "products"
)

silver_products = transform_products(
    products_raw,
    products_run["run_id"]
)

display(silver_products)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

products_count = validate_silver_data(

    silver_products,

    ["product_id"],

    "Products"
)


upsert_delta_table(

    silver_products,

    "silver_products",

    ["product_id"]
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(
    spark.sql("""
        SELECT *
        FROM silver_products
        LIMIT 20
    """)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

carts_raw, carts_run = read_bronze_data(
    "carts"
)

display(carts_raw)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def transform_cart_header(
    raw_df,
    run_id
):

    cart_array = raw_df.select(
        F.explode("carts").alias("cart")
    )

    silver_df = cart_array.select(

        F.col("cart.id")
            .cast("int")
            .alias("cart_id"),

        F.col("cart.userId")
            .cast("int")
            .alias("user_id"),

        F.col("cart.total")
            .cast("decimal(18,2)")
            .alias("cart_total"),

        F.col("cart.discountedTotal")
            .cast("decimal(18,2)")
            .alias("discounted_total"),

        F.col("cart.totalProducts")
            .cast("int")
            .alias("total_products"),

        F.col("cart.totalQuantity")
            .cast("int")
            .alias("total_quantity"),

        F.lit(run_id)
            .alias("source_run_id"),

        F.current_timestamp()
            .alias("processed_timestamp")
    )

    return silver_df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_carts = transform_cart_header(
    carts_raw,
    carts_run["run_id"]
)

display(silver_carts)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def transform_cart_items(
    raw_df,
    run_id
):

    cart_array = raw_df.select(
        F.explode("carts").alias("cart")
    )

    cart_items_array = cart_array.select(

        F.col("cart.id")
            .cast("int")
            .alias("cart_id"),

        F.explode("cart.products")
            .alias("item")
    )

    silver_df = cart_items_array.select(

        F.col("cart_id"),

        F.col("item.id")
            .cast("int")
            .alias("product_id"),

        F.col("item.title")
            .cast("string")
            .alias("product_name"),

        F.col("item.price")
            .cast("decimal(18,2)")
            .alias("unit_price"),

        F.col("item.quantity")
            .cast("int")
            .alias("quantity"),

        F.col("item.total")
            .cast("decimal(18,2)")
            .alias("line_total"),

        F.lit(run_id)
            .alias("source_run_id"),

        F.current_timestamp()
            .alias("processed_timestamp")
    )

    return silver_df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_cart_items = transform_cart_items(
    carts_raw,
    carts_run["run_id"]
)

display(silver_cart_items)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Validate cart headers

validate_silver_data(

    silver_carts,

    ["cart_id"],

    "Cart Headers"
)


# Validate cart line items

validate_silver_data(

    silver_cart_items,

    ["cart_id", "product_id"],

    "Cart Line Items"
)


# Load cart headers

upsert_delta_table(

    silver_carts,

    "silver_carts",

    ["cart_id"]
)


# Load cart items

upsert_delta_table(

    silver_cart_items,

    "silver_cart_items",

    ["cart_id", "product_id"]
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def transform_users(
    raw_df,
    run_id
):

    users_df = raw_df.select(
        F.explode("users").alias("user")
    )

    silver_df = users_df.select(

        F.col("user.id")
            .cast("int")
            .alias("user_id"),

        F.col("user.firstName")
            .alias("first_name"),

        F.col("user.lastName")
            .alias("last_name"),

        F.col("user.email")
            .alias("email"),

        F.col("user.phone")
            .alias("phone"),

        F.col("user.age")
            .cast("int")
            .alias("age"),

        F.col("user.gender")
            .alias("gender"),

        F.col("user.address.city")
            .alias("city"),

        F.col("user.address.state")
            .alias("state"),

        F.col("user.address.country")
            .alias("country"),

        F.col("user.company.name")
            .alias("company_name"),

        F.lit(run_id)
            .alias("source_run_id"),

        F.current_timestamp()
            .alias("processed_timestamp")
    )

    return silver_df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

validate_silver_data(

    silver_users,

    ["user_id"],

    "Users"
)


upsert_delta_table(

    silver_users,

    "silver_users",

    ["user_id"]
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_audit_schema = StructType([

    StructField("source_name", StringType(), True),

    StructField("source_run_id", StringType(), True),

    StructField("target_table", StringType(), True),

    StructField("records_processed", LongType(), True),

    StructField("status", StringType(), True),

    StructField("error_message", StringType(), True),

    StructField("processed_at", StringType(), True)

])


def write_silver_audit(

    source_name,
    source_run_id,
    target_table,
    records_processed,
    status,
    error_message=None
):

    audit_record = [(
        source_name,
        source_run_id,
        target_table,
        records_processed,
        status,
        error_message,
        datetime.now(timezone.utc).isoformat()
    )]

    audit_df = spark.createDataFrame(
        audit_record,
        silver_audit_schema
    )

    audit_df.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable("etl_silver_audit")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def process_entity(entity):

    source_name = entity["source_name"]

    target_table = entity["target_table"]

    transform_type = entity["transform_type"].upper()

    primary_key = [
        key.strip()
        for key in entity["primary_key"].split(",")
    ]

    run_id = None

    try:

        print("\n================================")
        print("Processing:", source_name)
        print("Transformation:", transform_type)
        print("Target:", target_table)
        print("================================")

        # Read latest successful Bronze execution
        raw_df, run_details = read_bronze_data(
            source_name
        )

        run_id = run_details["run_id"]

        # --------------------------------
        # PRODUCTS
        # --------------------------------

        if transform_type == "PRODUCT":

            silver_df = transform_products(
                raw_df,
                run_id
            )

            record_count = validate_silver_data(
                silver_df,
                primary_key,
                source_name
            )

            upsert_delta_table(
                silver_df,
                target_table,
                primary_key
            )

            write_silver_audit(
                source_name,
                run_id,
                target_table,
                record_count,
                "SUCCESS"
            )

        # --------------------------------
        # CARTS
        # --------------------------------

        elif transform_type == "CART":

            # Cart header
            header_df = transform_cart_header(
                raw_df,
                run_id
            )

            header_count = validate_silver_data(
                header_df,
                primary_key,
                "Cart Headers"
            )

            upsert_delta_table(
                header_df,
                target_table,
                primary_key
            )

            write_silver_audit(
                source_name,
                run_id,
                target_table,
                header_count,
                "SUCCESS"
            )


            # Cart items
            items_df = transform_cart_items(
                raw_df,
                run_id
            )

            items_count = validate_silver_data(
                items_df,
                ["cart_id", "product_id"],
                "Cart Items"
            )

            upsert_delta_table(
                items_df,
                "silver_cart_items",
                ["cart_id", "product_id"]
            )

            write_silver_audit(
                source_name,
                run_id,
                "silver_cart_items",
                items_count,
                "SUCCESS"
            )

        # --------------------------------
        # USERS
        # --------------------------------

        elif transform_type == "USER":

            silver_df = transform_users(
                raw_df,
                run_id
            )

            record_count = validate_silver_data(
                silver_df,
                primary_key,
                source_name
            )

            upsert_delta_table(
                silver_df,
                target_table,
                primary_key
            )

            write_silver_audit(
                source_name,
                run_id,
                target_table,
                record_count,
                "SUCCESS"
            )

        else:

            raise ValueError(
                f"Unsupported transformation: {transform_type}"
            )

        print(
            f"Completed successfully: {source_name}"
        )

        return {
            "source_name": source_name,
            "source_run_id": run_id,
            "status": "SUCCESS"
        }

    except Exception as e:

        print(
            f"Transformation failed: {source_name}"
        )

        print(str(e))

        write_silver_audit(
            source_name,
            run_id,
            target_table,
            0,
            "FAILED",
            str(e)[:4000]
        )

        return {
            "source_name": source_name,
            "source_run_id": run_id,
            "status": "FAILED",
            "error_message": str(e)
        }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_results = []

for entity in entity_config:

    source_name = entity["source_name"]

    # Ensure corresponding API source is enabled
    if source_name not in api_config:

        print(
            f"Skipping {source_name}: "
            "API source is not enabled."
        )

        continue

    result = process_entity(entity)

    silver_results.append(result)


print("SILVER EXECUTION SUMMARY")

silver_results_df = spark.createDataFrame(
    silver_results
)

display(silver_results_df)


failed_entities = [
    result
    for result in silver_results
    if result["status"] == "FAILED"
]

if failed_entities:

    raise Exception(
        f"{len(failed_entities)} entity transformations failed. "
        "Check etl_silver_audit."
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_tables = [
    "silver_products",
    "silver_carts",
    "silver_cart_items",
    "silver_users"
]

for table_name in silver_tables:

    if spark.catalog.tableExists(table_name):

        count = spark.table(
            table_name
        ).count()

        print(
            f"{table_name}: {count} records"
        )

    else:

        print(
            f"{table_name}: TABLE NOT FOUND"
        )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(
    spark.sql("""
        SELECT
            source_name,
            source_run_id,
            target_table,
            records_processed,
            status,
            error_message,
            processed_at
        FROM etl_silver_audit
        ORDER BY processed_at DESC
    """)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

cart_reconciliation = spark.sql("""

    SELECT

        c.cart_id,

        c.total_products,

        COUNT(i.product_id) AS actual_product_count,

        c.total_quantity,

        COALESCE(SUM(i.quantity), 0) AS actual_quantity,

        c.cart_total,

        COALESCE(SUM(i.line_total), 0) AS calculated_cart_total

    FROM silver_carts c

    LEFT JOIN silver_cart_items i

        ON c.cart_id = i.cart_id

    GROUP BY

        c.cart_id,

        c.total_products,

        c.total_quantity,

        c.cart_total

""")

display(cart_reconciliation)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
