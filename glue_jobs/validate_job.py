import sys
from functools import reduce

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, lit, current_timestamp
from pyspark.sql import DataFrame

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

quarantine_base = f"s3://{bucket_name}/quarantine/"
validation_output_path = f"s3://{bucket_name}/validation_results/run_id={run_id}/"

# -------------------------------------------------------------------
# Read standardized datasets
# -------------------------------------------------------------------
staffing_df = spark.read.parquet(staffing_path)
provider_df = spark.read.parquet(provider_path)

# -------------------------------------------------------------------
# Helper
# -------------------------------------------------------------------
def write_quarantine(df: DataFrame, reason: str):
    if df.take(1):
        (
            df.withColumn("quarantine_reason", lit(reason))
              .withColumn("run_id", lit(run_id))
              .withColumn("quarantine_ts_utc", current_timestamp())
              .write
              .mode("overwrite")
              .parquet(f"{quarantine_base}reason={reason}/run_id={run_id}/")
        )

def assert_required_columns(df: DataFrame, required_columns: list, dataset_name: str):
    missing = [c for c in required_columns if c not in df.columns]
    if missing:
        raise ValueError(f"{dataset_name} is missing required columns: {missing}")

# -------------------------------------------------------------------
# Required columns
# -------------------------------------------------------------------
staffing_required = ["provnum", "workdate"]
provider_required = ["cms_certification_number_ccn"]

assert_required_columns(staffing_df, staffing_required, "staffing_df")
assert_required_columns(provider_df, provider_required, "provider_df")

# -------------------------------------------------------------------
# Dataset not empty
# -------------------------------------------------------------------
staffing_count = staffing_df.count()
provider_count = provider_df.count()

if staffing_count == 0:
    raise ValueError("Staffing dataset is empty.")

if provider_count == 0:
    raise ValueError("Provider dataset is empty.")

# -------------------------------------------------------------------
# Null key checks
# -------------------------------------------------------------------
bad_staffing_required = staffing_df.filter(
    col("provnum").isNull() | col("workdate").isNull()
)

bad_provider_required = provider_df.filter(
    col("cms_certification_number_ccn").isNull()
)

write_quarantine(bad_staffing_required, "missing_required_staffing_fields")
write_quarantine(bad_provider_required, "missing_required_provider_key")

bad_staffing_required_count = bad_staffing_required.count()
bad_provider_required_count = bad_provider_required.count()

# -------------------------------------------------------------------
# Duplicate key checks
# -------------------------------------------------------------------
staffing_duplicate_keys = (
    staffing_df.groupBy("provnum", "workdate")
    .count()
    .filter(col("count") > 1)
)

provider_duplicate_keys = (
    provider_df.groupBy("cms_certification_number_ccn")
    .count()
    .filter(col("count") > 1)
)

staffing_duplicate_rows = staffing_df.join(
    staffing_duplicate_keys.select("provnum", "workdate"),
    on=["provnum", "workdate"],
    how="inner"
)

provider_duplicate_rows = provider_df.join(
    provider_duplicate_keys.select("cms_certification_number_ccn"),
    on=["cms_certification_number_ccn"],
    how="inner"
)

write_quarantine(staffing_duplicate_rows, "duplicate_staffing_keys")
write_quarantine(provider_duplicate_rows, "duplicate_provider_keys")

staffing_duplicate_count = staffing_duplicate_rows.count()
provider_duplicate_count = provider_duplicate_rows.count()

# -------------------------------------------------------------------
# Negative hours checks
# -------------------------------------------------------------------
hour_columns = [
    "hrs_rndon",
    "hrs_rndon_emp",
    "hrs_rndon_ctr",
    "hrs_rnadmin",
    "hrs_rnadmin_emp",
    "hrs_rnadmin_ctr",
    "hrs_rn",
    "hrs_rn_emp",
    "hrs_rn_ctr",
    "hrs_lpnadmin",
    "hrs_lpnadmin_emp",
    "hrs_lpnadmin_ctr",
    "hrs_lpn",
    "hrs_lpn_emp",
    "hrs_lpn_ctr",
    "hrs_cna",
    "hrs_cna_emp",
    "hrs_cna_ctr",
    "hrs_natrn",
    "hrs_natrn_emp",
    "hrs_natrn_ctr",
    "hrs_medaide",
    "hrs_medaide_emp",
    "hrs_medaide_ctr"
]

existing_hour_columns = [c for c in hour_columns if c in staffing_df.columns]

if not existing_hour_columns:
    raise ValueError("No expected staffing hour columns were found in the staffing dataset.")

negative_conditions = [col(c) < 0 for c in existing_hour_columns]
negative_hours_filter = reduce(lambda a, b: a | b, negative_conditions)

negative_hours_rows = staffing_df.filter(negative_hours_filter)

write_quarantine(negative_hours_rows, "negative_staffing_hours")

negative_hours_count = negative_hours_rows.count()

# -------------------------------------------------------------------
# Referential integrity check
# staffing provnum must exist in provider cms_certification_number_ccn
# -------------------------------------------------------------------
unmatched_staffing_rows = staffing_df.join(
    provider_df.select("cms_certification_number_ccn").dropDuplicates(),
    staffing_df["provnum"] == provider_df["cms_certification_number_ccn"],
    how="left_anti"
)

write_quarantine(unmatched_staffing_rows, "unmatched_provider_reference_warning")

unmatched_staffing_count = unmatched_staffing_rows.count()

# -------------------------------------------------------------------
# Write validation summary
# -------------------------------------------------------------------
summary_rows = [
    ("staffing_row_count", staffing_count, "info"),
    ("provider_row_count", provider_count, "info"),
    ("bad_staffing_required_count", bad_staffing_required_count, "critical"),
    ("bad_provider_required_count", bad_provider_required_count, "critical"),
    ("staffing_duplicate_count", staffing_duplicate_count, "critical"),
    ("provider_duplicate_count", provider_duplicate_count, "critical"),
    ("negative_hours_count", negative_hours_count, "critical"),
    ("unmatched_staffing_count", unmatched_staffing_count, "warning")
]

summary_df = spark.createDataFrame(summary_rows, ["metric_name", "metric_value", "severity"])

(
    summary_df.write
    .mode("overwrite")
    .parquet(validation_output_path)
)

# -------------------------------------------------------------------
# Fail job on critical issues
# -------------------------------------------------------------------
critical_failures = (
    bad_staffing_required_count +
    bad_provider_required_count +
    staffing_duplicate_count +
    provider_duplicate_count +
    negative_hours_count
)

if critical_failures > 0:
    raise ValueError(
        f"Validation failed. Critical issue count = {critical_failures}. "
        f"See quarantine and validation_results for details. "
    )

job.commit()