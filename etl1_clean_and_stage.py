# etl1_clean_and_stage.py
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import Window
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, regexp_replace, row_number, trim, split, concat_ws, upper, lower,
    when, regexp_extract, to_date, year, month, lit, size, slice
)
from pyspark.sql.types import DateType

# ----- GLUE SETUP -----
args = getResolvedOptions(sys.argv, ['JOB_NAME', 's3_input_path', 's3_output_path'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

INPUT_PATH = args['s3_input_path']       # e.g., s3://tgtransport/inputTgTrans/
OUTPUT_ROOT = args['s3_output_path'].rstrip('/')  # e.g., s3://my-etl-bucket/output/
STAGE_PATH = f"{OUTPUT_ROOT}/stage_clean_source/"

# ---------------------------------------------------
# Helper Function: Parse and Standardize Date Columns
# ---------------------------------------------------
def parse_dates(df):
    """Clean and standardize both fromdate and todate columns."""
    print(" Cleaning and parsing fromdate & todate columns...")

    df = df.withColumn("fromdate_clean", trim(regexp_replace(col("fromdate").cast("string"), r"[^\d/.\-]", ""))) \
           .withColumn("todate_clean", trim(regexp_replace(col("todate").cast("string"), r"[^\d/.\-]", ""))) \
           .withColumn("fromdate_clean", regexp_replace("fromdate_clean", r"[\.-]", "/")) \
           .withColumn("todate_clean", regexp_replace("todate_clean", r"[\.-]", "/")) \
           .withColumn("fromdate_parsed",
               when(col("fromdate_clean").rlike(r"^\d{2}/\d{2}/\d{4}$"), to_date(col("fromdate_clean"), "dd/MM/yyyy"))
               .when(col("fromdate_clean").rlike(r"^\d{2}/\d{2}/\d{2}$"), to_date(col("fromdate_clean"), "dd/MM/yy"))
               .when(col("fromdate_clean").rlike(r"^\d{4}/\d{2}/\d{2}$"), to_date(col("fromdate_clean"), "yyyy/MM/dd"))
               .otherwise(lit(None).cast("date"))
           ).withColumn("todate_parsed",
               when(col("todate_clean").rlike(r"^\d{2}/\d{2}/\d{4}$"), to_date(col("todate_clean"), "dd/MM/yyyy"))
               .when(col("todate_clean").rlike(r"^\d{2}/\d{2}/\d{2}$"), to_date(col("todate_clean"), "dd/MM/yy"))
               .when(col("todate_clean").rlike(r"^\d{4}/\d{2}/\d{2}$"), to_date(col("todate_clean"), "yyyy/MM/dd"))
               .otherwise(lit(None).cast("date"))
           ).drop("fromdate_clean", "todate_clean")

    print(" Date parsing complete.")
    return df

# ---------------------------------------------------
# 1️ READ RAW DATA
# ---------------------------------------------------
print(f" Reading raw data from: {INPUT_PATH}")
df_raw = spark.read.option("header", True).option("inferSchema", True).csv(INPUT_PATH)
print(f" Raw rows read: {df_raw.count()}")

# ---------------------------------------------------
# 2️ CLEAN MAKER NAME & DEDUPE BY tempRegistrationNumber
# ---------------------------------------------------
print(" Cleaning makerName and deduplicating records...")
df = df_raw.withColumn("makerName", regexp_replace("makerName", r"[\\.,]+$", ""))

window_spec = Window.partitionBy("tempRegistrationNumber").orderBy(col("fromdate").desc())
df = df.withColumn("row_num", row_number().over(window_spec)) \
       .filter(col("row_num") == 1).drop("row_num")

# ---------------------------------------------------
# 3️ FIX OfficeCd MISALIGNMENT
# ---------------------------------------------------
print(" Fixing OfficeCd misalignment...")
df = df.withColumn(
    "OfficeCd",
    when(
        col("fromdate").cast("string").rlike("(?i)^(RTA|UNIT OFFICE|MVI|DTO|ZONAL|TRANSPORT).*") & col("OfficeCd").isNull(),
        col("fromdate")
    ).otherwise(col("OfficeCd"))
).withColumn(
    "fromdate",
    when(
        col("fromdate").cast("string").rlike("(?i)^(RTA|UNIT OFFICE|MVI|DTO|ZONAL|TRANSPORT).*"),
        lit(None).cast("string")
    ).otherwise(col("fromdate"))
)

# Remove TS/TG short codes
df = df.withColumn("OfficeCd", when(col("OfficeCd").rlike("(?i)^(TS|TG)$"), lit(None)).otherwise(col("OfficeCd")))

# Fill remaining OfficeCd from fromdate if missing
df = df.withColumn("OfficeCd", when(col("OfficeCd").isNull() & col("fromdate").isNotNull(), col("fromdate")).otherwise(col("OfficeCd")))

# ---------------------------------------------------
# 4️ CLEAN MODEL DESCRIPTION
# ---------------------------------------------------
print(" Cleaning modelDesc and deriving model/variant...")
df = df.withColumn("modelDescClean", trim(regexp_replace(col("modelDesc"), r"[^A-Za-z0-9\s\+\-\(\)\./]", " ")))
df = df.withColumn("isTrailer", lower(col("modelDescClean")).rlike("trailer|trailor|tipper|tractor|tanker"))
df = df.withColumn("isElectric", lower(col("modelDescClean")).rlike(r"\b(ev|bov|electric|hybrid)\b"))

df = df.withColumn("modelWords", split(col("modelDescClean"), r"\s+")) \
       .withColumn("modelName", upper(when(col("isTrailer"), col("modelDescClean")).otherwise(col("modelWords")[0]))) \
       .withColumn("variant_words", slice(col("modelWords"), lit(2), size(col("modelWords")) - 1)) \
       .withColumn("variant", upper(when(col("isTrailer"), lit("TRAILER/TIPPER/TRACTOR/TANKER"))
            .otherwise(trim(concat_ws(" ", col("variant_words")))))) \
       .withColumn("variant", when((col("variant") == "") | col("variant").isNull(), lit("UNKNOWN")).otherwise(col("variant")))

# ---------------------------------------------------
# 5️ PARSE DATES (Using Helper)
# ---------------------------------------------------
df = parse_dates(df)

# ---------------------------------------------------
# 6️ NORMALIZE FUEL & MAKE YEAR
# ---------------------------------------------------
print(" Normalizing fuel and inferring makeYear...")
df = df.withColumn("fuel_clean", upper(when(col("fuel").rlike("BATTERY|ELECTRIC"), "ELECTRIC")
                           .when(col("fuel").rlike("PETROL|GASOLINE"), "PETROL")
                           .when(col("fuel").rlike("DIESEL"), "DIESEL")
                           .when(col("fuel").rlike("CNG"), "CNG")
                           .when(col("fuel").rlike("LPG"), "LPG")
                           .otherwise("UNKNOWN")))

df = df.withColumn("makeYear_inferred", regexp_extract(col("modelDescClean"), r'(19\d{2}|20[0-2]\d)', 0)) \
       .withColumn("makeYear", when((col("makeYear").isNull()) | (col("makeYear") == "") | (col("makeYear") == "UNKNOWN"),
                                    when(col("makeYear_inferred") != "", col("makeYear_inferred")).otherwise(lit("UNKNOWN"))
                                   ).otherwise(col("makeYear"))).drop("makeYear_inferred")

# ---------------------------------------------------
#  ADD PARTITIONS AND FILTER VALID RECORDS
# ---------------------------------------------------
print(" Adding partitions and filtering invalid records...")
df = df.withColumn("year", year("fromdate_parsed")).withColumn("month", month("fromdate_parsed"))
df_stage = df.filter(col("fromdate_parsed").isNotNull() & col("year").isNotNull() & col("month").isNotNull())

print(f" Stage rows (valid dates): {df_stage.count()}")

# ---------------------------------------------------
#  WRITE CLEAN STAGE DATA
# ---------------------------------------------------
print(f" Writing stage data to: {STAGE_PATH}")
df_stage.write.mode("overwrite").partitionBy("year", "month").parquet(STAGE_PATH)

print(" ETL1 job completed successfully.")
job.commit()
