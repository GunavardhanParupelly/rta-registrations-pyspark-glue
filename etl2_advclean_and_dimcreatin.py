"""
ETL2: Silver -> Gold (Star schema) for Vehicle Registration
- Glue-compatible script with robust argument handling.
- Fixes emissionStandard missing column issue.
- Outputs 4 Parquet files aligned to Redshift DWH star schema.
- Includes dynamic coalesce strategy for fact table.
- All major steps wrapped with try/except and Glue logging.
"""

import sys
import boto3
import math
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import Window
from pyspark.sql.functions import (
    col, lit, trim, upper, lower, regexp_replace, regexp_extract,
    sha2, concat_ws, when, year, expr, broadcast, levenshtein, row_number,
    coalesce, substring, length, concat
)
from pyspark.sql.types import BooleanType

# -----------------------------------------------------------
# 1. ARGUMENT HANDLING
# -----------------------------------------------------------
try:
    args = getResolvedOptions(sys.argv, ['JOB_NAME', 's3_input_path', 's3_output_path'])
    INPUT_PATH = args['s3_input_path'].rstrip('/')
    OUTPUT_ROOT = args['s3_output_path'].rstrip('/')
except Exception as e:
    print(f"FATAL ERROR: Missing required job parameters: {e}")
    sys.exit(1)

# -----------------------------
# 2. Glue Session Setup
# -----------------------------
sc = SparkContext()
glueContext = GlueContext(sc)
logger = glueContext.get_logger()
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")

logger.info(f"Job started: {args['JOB_NAME']}")

# -----------------------------
# 3. Define Output Paths and Constants
# -----------------------------
VEHICLE_DIM_PATH = f"{OUTPUT_ROOT}/gold_dim_vehicle/"
MANUFACTURER_DIM_PATH = f"{OUTPUT_ROOT}/gold_dim_manufacturer/"
RTA_DIM_PATH = f"{OUTPUT_ROOT}/gold_dim_rta/"
FACT_TEMP_PATH = f"{OUTPUT_ROOT}/gold_fact_registrations_temp/"
FACT_FINAL_PATH = f"{OUTPUT_ROOT}/gold_fact_registrations/"

FUZZY_THRESHOLD = 3
HASH_BITS = 256
TARGET_FILE_SIZE_MB = 128.0

# -----------------------------------------------------------
# Helper: S3 total size
# -----------------------------------------------------------
def get_s3_total_size(s3_path):
    """Calculate total size in bytes of Parquet objects under S3 path."""
    try:
        s3_client = boto3.client('s3')
        path_cleaned = s3_path.replace("s3://", "")
        if '/' in path_cleaned:
            bucket, prefix_raw = path_cleaned.split("/", 1)
            prefix = prefix_raw + ("/" if not prefix_raw.endswith("/") else "")
        else:
            bucket = path_cleaned
            prefix = ''
        total_size = 0
        paginator = s3_client.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            if 'Contents' in page:
                for obj in page['Contents']:
                    if obj['Key'].endswith('.parquet') or '/part-' in obj['Key']:
                        total_size += obj['Size']
        return total_size
    except Exception as e:
        logger.warn(f" Error calculating S3 size for {s3_path}: {e}")
        return 0

# -----------------------------------------------------------
# 4. Read ETL1 / Stage Data
# -----------------------------------------------------------
try:
    df_stage = spark.read.parquet(INPUT_PATH)
    logger.info(f" Loaded staged data rows: {df_stage.count()}")
except Exception as e:
    logger.error(f"FATAL ERROR: Failed to read input path {INPUT_PATH}: {e}")
    job.commit()
    sys.exit(1)

# -----------------------------
# 5. Column Standardization & Emission Fix
# -----------------------------
try:
    df = df_stage \
        .withColumn("makerName", trim(col("makerName"))) \
        .withColumn("modelName", trim(col("modelName"))) \
        .withColumn("variant", trim(col("variant"))) \
        .withColumn("OfficeCd", trim(col("OfficeCd")))

    # Two-digit year fix
    df = df.withColumn("makeYear_clean", col("makeYear").cast("string"))
    df = df.withColumn(
        "makeYear_clean",
        when((length(col("makeYear_clean")) == 2) & col("makeYear_clean").rlike("^[0-9]{2}$"),
             concat(lit("20"), col("makeYear_clean"))).otherwise(col("makeYear_clean"))
    )

    # Emission standard
    if "emissionStandard" not in df.columns:
        logger.warn("emissionStandard not present. Deriving/Defaulting...")
        temp_df = df.withColumn(
            "emissionStandard_raw",
            when(col("modelDescClean").isNotNull(),
                 regexp_extract(col("modelDescClean"), r"(BS\s?III[AB]?|BS\s?IV|BS\s?V|BS\s?VI)", 1)
            ).otherwise(lit(None))
        )
        df = temp_df.withColumn(
            "emissionStandard",
            when(col("isElectric") == True, lit("ELECTRIC"))
            .when((col("emissionStandard_raw").isNull()) | (col("emissionStandard_raw") == ""), lit("UNKNOWN"))
            .otherwise(upper(regexp_replace(col("emissionStandard_raw"), "\\s", "")))
        ).drop("emissionStandard_raw")
    else:
        df = df.withColumn("emissionStandard", when(col("emissionStandard").isNull(), lit("UNKNOWN")).otherwise(col("emissionStandard")))

    # Ensure boolean isElectric exists
    if "isElectric" in df.columns:
        df = df.withColumn(
            "isElectric",
            when((col("isElectric").cast("string") == "True") | (col("isElectric").cast("string") == "1"), lit(True))
            .otherwise(lit(False)).cast(BooleanType())
        )
    else:
        df = df.withColumn("isElectric", lit(False).cast(BooleanType()))

except Exception as e:
    logger.error(f"Error during column standardization: {e}")
    job.commit()
    sys.exit(1)

# -----------------------------
# 6. Surrogate Keys
# -----------------------------
try:
    df = df.withColumn("MAKE_YEAR_KEY", when(col("makeYear_clean").isNull(), lit("UNKNOWN")).otherwise(col("makeYear_clean")))
    df = df.withColumn("VEHICLE_KEY_SOURCE", concat_ws("|", lower(trim(col("modelName"))), lower(trim(col("variant"))), col("MAKE_YEAR_KEY")))
    df = df.withColumn("VEHICLE_ID", sha2(col("VEHICLE_KEY_SOURCE"), HASH_BITS))
    df = df.withColumn("MANUFACTURER_KEY_SOURCE", lower(trim(col("makerName")))) \
           .withColumn("MANUFACTURER_ID", sha2(col("MANUFACTURER_KEY_SOURCE"), HASH_BITS))
    df = df.withColumn("RTA_KEY_SOURCE", lower(trim(col("OfficeCd")))) \
           .withColumn("RTA_ID", sha2(col("RTA_KEY_SOURCE"), HASH_BITS))
except Exception as e:
    logger.error(f"Error during surrogate key generation: {e}")
    job.commit()
    sys.exit(1)

# -----------------------------
# 7. Build Dimensions
# -----------------------------
try:
    logger.info(" Building dimensions...")
    dim_vehicle = df.select(
        col("VEHICLE_ID"), col("modelName").alias("MODEL_NAME"),
        col("variant").alias("VARIANT"), col("emissionStandard").alias("EMISSION_STANDARD"),
        col("fuel_clean").alias("FUEL"), coalesce(col("colour"), lit("UNKNOWN")).alias("COLOUR"),
        coalesce(col("vehicleClass"), lit("UNKNOWN")).alias("VEHICLE_CLASS"),
        col("makeYear_clean").alias("MAKE_YEAR"), coalesce(col("seatCapacity").cast("int"), lit(0)).alias("SEAT_CAPACITY"),
        col("isElectric").alias("IS_ELECTRIC")
    ).dropDuplicates(["VEHICLE_ID"])

    dim_manufacturer = df.select(col("MANUFACTURER_ID"), col("makerName").alias("MAKER_NAME")).dropDuplicates(["MANUFACTURER_ID"])
    dim_rta = df.select(col("RTA_ID"), col("OfficeCd").alias("RTA_OFFICE_CODE")).dropDuplicates(["RTA_ID"])
    dim_rta = dim_rta.withColumn("RTA_REGION", lit(None).cast("string")) \
                     .withColumn("RTA_STATE", lit(None).cast("string")) \
                     .withColumn("RTA_CITY", lit(None).cast("string"))
except Exception as e:
    logger.error(f"Error building dimensions: {e}")
    job.commit()
    sys.exit(1)

# -----------------------------
# 8. Vehicle Resolution (Exact + Fuzzy)
# -----------------------------
try:
    logger.info(" Running vehicle resolution (Exact + Fuzzy)...")
    df_for_match = df.select("tempRegistrationNumber", "VEHICLE_ID", "modelName", "variant", "MAKE_YEAR_KEY").dropDuplicates(["tempRegistrationNumber"])
    dv_lookup = dim_vehicle.select(
        col("VEHICLE_ID"),
        lower(trim(col("MODEL_NAME"))).alias("dv_model"),
        lower(trim(col("VARIANT"))).alias("dv_variant"),
        col("MAKE_YEAR").alias("dv_make_year")
    ).withColumn("dv_make_year", col("dv_make_year").cast("string"))

    # Exact match
    joined_exact = df_for_match.alias("s").join(
        broadcast(dv_lookup).alias("dv"),
        (lower(trim(col("s.modelName"))) == col("dv.dv_model")) &
        (lower(trim(col("s.variant"))) == col("dv.dv_variant")) &
        (col("s.MAKE_YEAR_KEY") == col("dv.dv_make_year")),
        how="left"
    ).select(col("s.tempRegistrationNumber"), col("dv.VEHICLE_ID").alias("VEHICLE_ID_exact"))
    resolved_exact = joined_exact.filter(col("VEHICLE_ID_exact").isNotNull())

    # Fuzzy match (only unresolved)
    all_reg = df_for_match.select("tempRegistrationNumber").distinct()
    resolved_keys = resolved_exact.select("tempRegistrationNumber").distinct()
    unresolved_keys = all_reg.join(resolved_keys, on="tempRegistrationNumber", how="left_anti")

    unresolved = unresolved_keys.join(df_for_match, on="tempRegistrationNumber", how="inner") \
        .withColumn("FUZZY_KEY", lower(trim(concat_ws(" ", col("modelName"), col("variant"))))) \
        .withColumn("BLOCK_KEY", substring(lower(trim(col("modelName"))), 1, 2)) \
        .filter(length(col("BLOCK_KEY")) >= 2)

    dv_fuzzy = dv_lookup.withColumn("DV_FUZZY_KEY", lower(trim(concat_ws(" ", col("dv_model"), col("dv_variant"))))) \
        .withColumn("BLOCK_KEY", substring(col("dv_model"), 1, 2)).filter(length(col("BLOCK_KEY")) >= 2) \
        .select("VEHICLE_ID", "DV_FUZZY_KEY", "BLOCK_KEY", "dv_make_year")

    fuzzy_candidates = unresolved.alias("u").join(
        broadcast(dv_fuzzy).alias("dv"),
        (col("u.BLOCK_KEY") == col("dv.BLOCK_KEY")) & (col("u.MAKE_YEAR_KEY") == col("dv.dv_make_year")),
        how="inner"
    ).withColumn("LEV_DIST", levenshtein(col("u.FUZZY_KEY"), col("dv.DV_FUZZY_KEY"))) \
     .filter(col("LEV_DIST") <= FUZZY_THRESHOLD)

    fuzzy_window = Window.partitionBy("tempRegistrationNumber").orderBy(col("LEV_DIST").asc(), col("dv.VEHICLE_ID").asc())
    best_fuzzy = fuzzy_candidates.withColumn("rn", row_number().over(fuzzy_window)).filter(col("rn") == 1) \
        .select("tempRegistrationNumber", col("dv.VEHICLE_ID").alias("VEHICLE_ID_fuzzy"), col("LEV_DIST"))

    exact_sel = resolved_exact.select("tempRegistrationNumber", col("VEHICLE_ID_exact").alias("VEHICLE_ID_resolved"))
    fuzzy_sel = best_fuzzy.select("tempRegistrationNumber", col("VEHICLE_ID_fuzzy").alias("VEHICLE_ID_resolved"), col("LEV_DIST"))

    all_resolved = exact_sel.unionByName(fuzzy_sel, allowMissingColumns=True)
    all_resolved = all_resolved.withColumn("IS_FUZZY_MATCH", when(col("VEHICLE_ID_resolved").isNotNull() & col("LEV_DIST").isNotNull(), lit(True)).otherwise(lit(False)))

    man_res = df.select("tempRegistrationNumber", "MANUFACTURER_ID").dropDuplicates(["tempRegistrationNumber"])
except Exception as e:
    logger.error(f"Error in vehicle resolution: {e}")
    job.commit()
    sys.exit(1)

# -----------------------------
# 9. Assemble Fact Table
# -----------------------------
try:
    fact_df = df.alias("s") \
        .join(all_resolved.alias("r"), on="tempRegistrationNumber", how="left") \
        .join(man_res.alias("m"), on="tempRegistrationNumber", how="left") \
        .select(
            coalesce(col("r.VEHICLE_ID_resolved"), col("s.VEHICLE_ID")).alias("VEHICLE_ID"), 
            col("m.MANUFACTURER_ID").alias("MANUFACTURER_ID"),
            col("s.RTA_ID").alias("RTA_ID"),
            expr("CAST(date_format(s.fromdate_parsed, 'yyyyMMdd') AS INT)").alias("REGISTRATION_ISSUE_DATE_ID"),
            expr("CAST(date_format(s.todate_parsed, 'yyyyMMdd') AS INT)").alias("REGISTRATION_EXPIRY_DATE_ID"),
            year(col("s.fromdate_parsed")).alias("REGISTRATION_YEAR"),
            when(col("s.makeYear_clean").rlike("^[0-9]{4}$"), expr("CAST(CONCAT(s.makeYear_clean, '0101') AS INT)"))
                .otherwise(lit(None).cast("int")).alias("MANUFACTURER_DATE_ID"),
            col("s.vehicleClass").alias("TRANSPORT_TYPE"),
            col("s.tempRegistrationNumber").alias("TEMP_REGISTRATION_NUMBER"),
            col("s.slno").alias("SLNO"),
            coalesce(col("r.IS_FUZZY_MATCH"), lit(False)).alias("IS_FUZZY_MATCH"),
            col("s.colour").alias("COLOUR"),
            col("s.fuel_clean").alias("FUEL_TYPE"),
            col("s.modelName").alias("MODEL_NAME")
        ).filter(col("REGISTRATION_ISSUE_DATE_ID").isNotNull())
except Exception as e:
    logger.error(f"Error assembling fact table: {e}")
    job.commit()
    sys.exit(1)

# -----------------------------
# 10. Write Initial Fact Table & Dimensions
# -----------------------------
try:
    fact_df.repartition(10).write.mode("overwrite").partitionBy("REGISTRATION_YEAR").parquet(FACT_TEMP_PATH)
    logger.info(f" Wrote initial fact table to: {FACT_TEMP_PATH}")

    dim_vehicle.write.mode("overwrite").parquet(VEHICLE_DIM_PATH)
    dim_manufacturer.write.mode("overwrite").parquet(MANUFACTURER_DIM_PATH)
    dim_rta.write.mode("overwrite").parquet(RTA_DIM_PATH)
    logger.info(f"Gold dimensions written to: {OUTPUT_ROOT}")
except Exception as e:
    logger.error(f"Error writing fact or dimension tables: {e}")
    job.commit()
    sys.exit(1)

# -----------------------------
# 11. Dynamic Coalesce Fact Table
# -----------------------------
try:
    total_bytes = get_s3_total_size(FACT_TEMP_PATH)
    TARGET_BYTES = TARGET_FILE_SIZE_MB * 1024 * 1024
    MIN_PARTS = 1

    if total_bytes < (TARGET_BYTES / 2):
        optimal_parts = MIN_PARTS
    else:
        optimal_parts = min(100, max(MIN_PARTS, math.ceil(total_bytes / TARGET_BYTES)))

    logger.info(f" Total size: {total_bytes/1024/1024:.2f} MB. Coalescing to ~{optimal_parts} files per partition.")
    
    df_fact_coalesce = spark.read.parquet(FACT_TEMP_PATH)
    df_fact_coalesce.coalesce(optimal_parts) \
        .write.mode("overwrite") \
        .partitionBy("REGISTRATION_YEAR").parquet(FACT_FINAL_PATH)
    logger.info(f" Coalesced fact table written to: {FACT_FINAL_PATH}")
except Exception as e:
    logger.error(f"Error during fact table coalescing: {e}")
    job.commit()
    sys.exit(1)

# -----------------------------
# 12. Cleanup Temporary Data
# -----------------------------
try:
    logger.info(f"▶ Cleaning up temporary S3 path: {FACT_TEMP_PATH}")
    s3_resource = boto3.resource('s3')
    bucket = FACT_TEMP_PATH.replace("s3://", "").split("/", 1)[0]
    prefix = FACT_TEMP_PATH.replace("s3://", "").split("/", 1)[1] + "/"
    s3_resource.Bucket(bucket).objects.filter(Prefix=prefix).delete()
    logger.info(" Temporary data cleaned up.")
except Exception as e:
    logger.warn(f" Failed to clean up temporary S3 data: {e}")

# -----------------------------
# 13. Final Commit
# -----------------------------
job.commit()
logger.info(" ETL2 completed successfully.")
