import sys

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import (
    col,
    lit,
    current_timestamp,
    to_date,
    year,
    month,
    dayofmonth
)

# -------------------------------------------------------------------
# Job arguments
# -------------------------------------------------------------------
args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "bucket_name", "run_id"]
)

job_name = args["JOB_NAME"]
bucket_name = args["bucket_name"]
run_id = args["run_id"]

# -------------------------------------------------------------------
# Spark / Glue setup
# -------------------------------------------------------------------
sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(job_name, args)

# -------------------------------------------------------------------
# Paths
# -------------------------------------------------------------------
staffing_path = f"s3://{bucket_name}/refined/nurse_staffing/staffing_hours/run_id={run_id}/"
provider_path = f"s3://{bucket_name}/refined/nurse_staffing/provider_reference/run_id={run_id}/"

dim_provider_output_path = f"s3://{bucket_name}/curated/nurse_staffing/dim_provider/"
fact_staffing_output_path = f"s3://{bucket_name}/curated/nurse_staffing/fact_staffing_daily/"

# -------------------------------------------------------------------
# Read standardized datasets
# -------------------------------------------------------------------
staffing_df = spark.read.parquet(staffing_path)
provider_df = spark.read.parquet(provider_path)

# -------------------------------------------------------------------
# Prepare provider reference dataset
# -------------------------------------------------------------------
provider_dim_base = (
    provider_df
    .withColumnRenamed("cms_certification_number_ccn", "provnum")
    .dropDuplicates(["provnum"])
)

# -------------------------------------------------------------------
# Prepare provider fallback records from staffing
# -------------------------------------------------------------------
staffing_provider_attributes = (
    staffing_df
    .select(
        "provnum",
        "provname",
        "city",
        "state",
        "county_name",
        "county_fips"
    )
    .dropDuplicates(["provnum"])
)

fallback_provider_records = staffing_provider_attributes.join(
    provider_dim_base.select("provnum"),
    on="provnum",
    how="left_anti"
)

fallback_provider_records = (
    fallback_provider_records
    .withColumnRenamed("provname","provider_name")
    .withColumnRenamed("city","city_town")
    .withColumnRenamed("county_name","county_parish")
)

# -------------------------------------------------------------------
# Add county_fips to provider records
# -------------------------------------------------------------------
staffing_provider_county_fips = (
    staffing_df
    .select(
        "provnum",
        "county_fips"
    )
    .dropDuplicates(["provnum"])
)

provider_dim_base = staffing_provider_county_fips.join(
    provider_dim_base,
    on="provnum",
    how="inner"
) 

# -------------------------------------------------------------------
# Add provenance flags to official provider records
# -------------------------------------------------------------------
provider_dim_base = (
    provider_dim_base
    .withColumn("provider_match_status", lit("matched"))
    .withColumn("provider_reference_source", lit("provider_file"))
    .withColumn("provider_reference_gap_flag", lit("N"))
    .withColumn("run_id", lit(run_id))
    .withColumn("curated_ts_utc", current_timestamp())
)

# -------------------------------------------------------------------
# Align fallback schema with provider dimension
# Keep provider file columns where available as null if missing in fallback
# -------------------------------------------------------------------
provider_base_columns = provider_dim_base.columns

for c in provider_base_columns:
    if c not in fallback_provider_records.columns:
        fallback_provider_records = fallback_provider_records.withColumn(c, lit(None).cast(provider_dim_base.schema[c].dataType))

fallback_provider_records = fallback_provider_records.select(provider_base_columns)

fallback_provider_records = (
    fallback_provider_records
    .withColumn("provider_match_status", lit("inferred_from_staffing"))
    .withColumn("provider_reference_source", lit("staffing_file"))
    .withColumn("provider_reference_gap_flag", lit("Y"))
    .withColumn("run_id", lit(run_id))
    .withColumn("curated_ts_utc", current_timestamp())
)

# -------------------------------------------------------------------
# Build final provider dimension
# -------------------------------------------------------------------
dim_provider_df = provider_dim_base.unionByName(fallback_provider_records)

# -------------------------------------------------------------------
# Prepare staffing fact
# -------------------------------------------------------------------
fact_staffing_df = (
    staffing_df
    .withColumn("work_date", to_date(col("workdate").cast("string"), "yyyyMMdd"))
    .withColumn("work_year", year(col("work_date")))
    .withColumn("work_month", month(col("work_date")))
    .withColumn("work_day", dayofmonth(col("work_date")))
    .withColumn("run_id", lit(run_id))
    .withColumn("curated_ts_utc", current_timestamp())
    .drop("workdate","provname","city","state","county_name","county_fips")
)

bad_workdate_count = fact_staffing_df.filter(col("work_date").isNull()).count()

if bad_workdate_count > 0:
    raise ValueError(f"work_date parsing failed for {bad_workdate_count} rows.")

# -------------------------------------------------------------------
# Write curated outputs
# -------------------------------------------------------------------
(
    dim_provider_df.write
    .mode("overwrite")
    .parquet(dim_provider_output_path)
)

(
    fact_staffing_df.write
    .mode("overwrite")
    .partitionBy("work_year", "work_month")
    .parquet(fact_staffing_output_path)
)

job.commit()