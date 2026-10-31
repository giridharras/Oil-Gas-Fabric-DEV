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
import requests
import json
import uuid
import time

from datetime import datetime, timezone
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from pyspark.sql import functions as F
from pyspark.sql.types import *
print("API Ingestion Framework Initialized")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# None = Process all enabled APIs
# "products" = Process only Products
# "carts" = Process only Carts
# "users" = Process only Users

p_source_name = None

# Optional execution identifier
p_execution_type = "MANUAL"

print("Source Filter:", p_source_name)
print("Execution Type:", p_execution_type)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def create_api_session():

    session = requests.Session()

    retry_strategy = Retry(

        total=5,

        connect=3,

        read=3,

        backoff_factor=2,

        status_forcelist=[
            429,
            500,
            502,
            503,
            504
        ],

        allowed_methods=frozenset(["GET"]),

        respect_retry_after_header=True
    )

    adapter = HTTPAdapter(
        max_retries=retry_strategy
    )

    session.mount("https://", adapter)
    session.mount("http://", adapter)

    return session


api_session = create_api_session()

print("API Session Created Successfully")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

CONFIG_TABLE = "cfg_api_source"

if not spark.catalog.tableExists(CONFIG_TABLE):

    raise Exception(
        "Configuration table not found. "
        "Execute NB_00_CREATE_CONFIG first."
    )


api_config_df = spark.table(CONFIG_TABLE)

# Only enabled APIs
api_config_df = api_config_df.filter(
    F.col("enabled") == True
)

# Optional single API execution
if p_source_name is not None:

    api_config_df = api_config_df.filter(
        F.col("source_name") == p_source_name
    )


api_sources = [
    row.asDict()
    for row in api_config_df.collect()
]

if not api_sources:

    raise Exception(
        "No enabled API sources found."
    )


print("Total APIs to process:", len(api_sources))

display(api_config_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def create_bronze_folder(source_name, run_id):

    ingestion_date = datetime.now(
        timezone.utc
    ).strftime("%Y-%m-%d")

    bronze_path = (
        f"Files/bronze/{source_name}/"
        f"ingest_date={ingestion_date}/"
        f"run_id={run_id}"
    )

    notebookutils.fs.mkdirs(bronze_path)

    return bronze_path

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


def write_audit_log(audit_record):

    audit_df = spark.createDataFrame(
        [audit_record],
        schema=audit_schema
    )

    audit_df.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable("etl_run_audit")

    print(
        f"Audit updated: "
        f"{audit_record['source_name']} - "
        f"{audit_record['status']}"
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def ingest_api_source(source):

    # --------------------------------
    # 1. Read configuration
    # --------------------------------

    source_name = source["source_name"]

    base_url = source["base_url"].rstrip("/")

    endpoint = source["endpoint"].lstrip("/")

    response_key = source["response_data_key"]

    page_size = int(source["page_size"])

    pagination_type = source["pagination_type"].upper()

    auth_type = source["auth_type"].upper()

    load_type = source["load_type"].upper()


    # --------------------------------
    # 2. Validate configuration
    # --------------------------------

    if pagination_type != "OFFSET":

        raise ValueError(
            f"Unsupported pagination: {pagination_type}"
        )

    if auth_type != "NONE":

        raise ValueError(
            f"Authentication not implemented: {auth_type}"
        )

    if load_type != "FULL":

        raise ValueError(
            f"Load type not implemented: {load_type}"
        )

    if page_size <= 0:

        raise ValueError(
            "Page size must be greater than zero"
        )


    # --------------------------------
    # 3. Initialize execution
    # --------------------------------

    run_id = str(uuid.uuid4())

    started_at = datetime.now(
        timezone.utc
    ).isoformat()

    bronze_path = create_bronze_folder(
        source_name,
        run_id
    )

    skip = 0

    page_number = 1

    total_records = 0

    total_pages = 0


    print("----------------------------------")
    print("Starting API:", source_name)
    print("Run ID:", run_id)
    print("Bronze Path:", bronze_path)
    print("----------------------------------")


    # --------------------------------
    # 4. API pagination loop
    # --------------------------------

    while True:

        url = f"{base_url}/{endpoint}"

        params = {
            "limit": page_size,
            "skip": skip
        }

        print(
            f"Calling API: {url} "
            f"| Page: {page_number}"
        )


        # API request
        response = api_session.get(

            url,

            params=params,

            timeout=60
        )

        response.raise_for_status()

        payload = response.json()


        # Validate API response
        if response_key not in payload:

            raise ValueError(
                f"Response key '{response_key}' "
                f"not found in API response"
            )

        records = payload[response_key]

        if not isinstance(records, list):

            raise TypeError(
                f"Expected a list in '{response_key}'"
            )


        # Stop without writing an empty page
        if len(records) == 0:

            break


        # --------------------------------
        # 5. Save raw API response
        # --------------------------------

        file_name = (
            f"page_{page_number:05d}.json"
        )

        file_path = (
            f"{bronze_path}/{file_name}"
        )

        notebookutils.fs.put(

            file_path,

            json.dumps(
                payload,
                ensure_ascii=False
            ),

            True
        )


        # --------------------------------
        # 6. Update counters
        # --------------------------------

        records_count = len(records)

        total_records += records_count

        total_pages += 1


        print(
            f"Saved {records_count} records "
            f"| Total: {total_records}"
        )


        # --------------------------------
        # 7. Pagination control
        # --------------------------------

        skip += records_count

        page_number += 1

        total_available = payload.get("total")

        if total_available is not None:

            if skip >= int(total_available):

                break

        else:

            if records_count < page_size:

                break


    # --------------------------------
    # 8. Return execution details
    # --------------------------------

    completed_at = datetime.now(
        timezone.utc
    ).isoformat()

    result = {

        "run_id": run_id,

        "source_name": source_name,

        "status": "SUCCESS",

        "records_processed": total_records,

        "pages_processed": total_pages,

        "bronze_path": bronze_path,

        "error_message": None,

        "started_at": started_at,

        "completed_at": completed_at
    }


    print("----------------------------------")
    print("API INGESTION COMPLETED")
    print("Source:", source_name)
    print("Records:", total_records)
    print("Pages:", total_pages)
    print("----------------------------------")

    return result

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


ingestion_results = []

failed_sources = []

for source in api_sources:

    source_name = source["source_name"]

    try:
        print(f"\nStarting ingestion for: {source_name}")

        # Execute API ingestion
        result = ingest_api_source(source)

        # Write SUCCESS audit
        write_audit_log(result)

        ingestion_results.append(result)

    except Exception as e:

        print(f"API FAILED: {source_name}")
        print(str(e))

        failure_record = {
            "run_id": str(uuid.uuid4()),
            "source_name": source_name,
            "status": "FAILED",
            "records_processed": 0,
            "pages_processed": 0,
            "bronze_path": None,
            "error_message": str(e)[:4000],
            "started_at": None,
            "completed_at": datetime.now(
                timezone.utc
            ).isoformat()
        }

        # Write FAILED audit
        write_audit_log(failure_record)

        failed_sources.append(failure_record)


print("\nAPI INGESTION SUMMARY")

# Explicit schema prevents Spark inference errors
summary_schema = StructType([
    StructField("run_id", StringType(), True),
    StructField("source_name", StringType(), True),
    StructField("status", StringType(), True),
    StructField("records_processed", LongType(), True),
    StructField("pages_processed", IntegerType(), True),
    StructField("bronze_path", StringType(), True),
    StructField("error_message", StringType(), True),
    StructField("started_at", StringType(), True),
    StructField("completed_at", StringType(), True)
])

# Display successful ingestion results
if ingestion_results:

    success_df = spark.createDataFrame(
        ingestion_results,
        schema=summary_schema
    )

    display(success_df)


# Display failed ingestion results
if failed_sources:

    failed_df = spark.createDataFrame(
        failed_sources,
        schema=summary_schema
    )

    display(failed_df)

    raise Exception(
        f"{len(failed_sources)} API sources failed. "
        "Check etl_run_audit."
    )

print("\nAll API sources processed successfully.")


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
            status,
            records_processed,
            pages_processed,
            bronze_path,
            started_at,
            completed_at
        FROM etl_run_audit
        ORDER BY completed_at DESC
    """)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
