import sys
import json
import re
import boto3

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, lit, current_timestamp, trim, when
from pyspark.sql import DataFrame
from pyspark.sql.types import (
    StringType,
    IntegerType,
    DoubleType,
    DecimalType,
    TimestampType
)

# -------------------------------------------------------------------
# Job arguments
# -------------------------------------------------------------------
args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "bucket_name", "manifest_key", "run_id"]
)

job_name = args["JOB_NAME"]
bucket_name = args["bucket_name"]
manifest_key = args["manifest_key"]
run_id = args["run_id"]

# -------------------------------------------------------------------
# Spark / Glue setup
# -------------------------------------------------------------------
sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(job_name, args)

s3_client = boto3.client("s3")


# -------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------
def load_manifest(bucket: str, key: str) -> dict:
    response = s3_client.get_object(Bucket=bucket, Key=key)
    content = response["Body"].read().decode("utf-8")
    return json.loads(content)


def standardize_column_name(col_name: str) -> str:
    col_name = col_name.strip().lower()
    col_name = re.sub(r"[^a-z0-9]+", "_", col_name)
    col_name = re.sub(r"_+", "_", col_name)
    col_name = col_name.strip("_")
    return col_name


def standardize_columns(df: DataFrame) -> DataFrame:
    new_cols = [standardize_column_name(col) for col in df.columns]
    return df.toDF(*new_cols)


def add_lineage_columns(df: DataFrame, dataset_name: str, source_file_name: str) -> DataFrame:
    return (
        df.withColumn("run_id", lit(run_id))
          .withColumn("dataset_name", lit(dataset_name))
          .withColumn("source_file_name", lit(source_file_name))
          .withColumn("ingest_ts_utc", current_timestamp())
    )

def normalize_blank_strings(df: DataFrame) -> DataFrame:
    """
    Convert blank strings to null and trim whitespace on string-like columns.
    """
    for c in df.columns:
        df = df.withColumn(
            c,
            when(trim(col(c).cast("string")) == "", None).otherwise(trim(col(c).cast("string")))
        )
    return df


def cast_columns(df: DataFrame, column_type_map: dict, dataset_name: str) -> DataFrame:
    """
    Cast columns to explicit Spark data types if they exist in the DataFrame.
    """
    for c, dtype in column_type_map.items():
        if c in df.columns:
            df = df.withColumn(c, col(c).cast(dtype))
        else:
            print(f"[WARN] Column '{c}' not found in {dataset_name}; skipping cast.")
    return df

staffing_column_type_map = {
    "provnum": StringType(),
    "provname": StringType(),
    "city": StringType(),
    "state": StringType(),
    "county_name": StringType(),
    "county_fips": StringType(),
    "cy_qtr": StringType(),
    "workdate": StringType(),
    "mdscensus": IntegerType(),
    "hrs_rndon": DecimalType(10, 2),
    "hrs_rndon_emp": DecimalType(10, 2),
    "hrs_rndon_ctr": DecimalType(10, 2),
    "hrs_rnadmin": DecimalType(10, 2),
    "hrs_rnadmin_emp": DecimalType(10, 2),
    "hrs_rnadmin_ctr": DecimalType(10, 2),
    "hrs_rn": DecimalType(10, 2),
    "hrs_rn_emp": DecimalType(10, 2),
    "hrs_rn_ctr": DecimalType(10, 2),
    "hrs_lpnadmin": DecimalType(10, 2),
    "hrs_lpnadmin_emp": DecimalType(10, 2),
    "hrs_lpnadmin_ctr": DecimalType(10, 2),
    "hrs_lpn": DecimalType(10, 2),
    "hrs_lpn_emp": DecimalType(10, 2),
    "hrs_lpn_ctr": DecimalType(10, 2),
    "hrs_cna": DecimalType(10, 2),
    "hrs_cna_emp": DecimalType(10, 2),
    "hrs_cna_ctr": DecimalType(10, 2),
    "hrs_natrn": DecimalType(10, 2),
    "hrs_natrn_emp": DecimalType(10, 2),
    "hrs_natrn_ctr": DecimalType(10, 2),
    "hrs_medaide": DecimalType(10, 2),
    "hrs_medaide_emp": DecimalType(10, 2),
    "hrs_medaide_ctr": DecimalType(10, 2)
}

provider_column_type_map = {
    "cms_certification_number_ccn": StringType(),
    "provider_name": StringType(),
    "provider_address": StringType(),
    "city_town": StringType(),
    "state": StringType(),
    "zip_code": StringType(),
    "telephone_number": StringType(),
    "provider_ssa_county_code": StringType(),
    "county_parish": StringType(),
    "ownership_type": StringType(),
    "number_of_certified_beds": IntegerType(),
    "average_number_of_residents_per_day": DecimalType(6, 2),
    "average_number_of_residents_per_day_footnote": StringType(),
    "provider_type": StringType(),
    "provider_resides_in_hospital": StringType(),
    "legal_business_name": StringType(),
    "date_first_approved_to_provide_medicare_and_medicaid_services": StringType(),
    "affiliated_entity_name": StringType(),
    "affiliated_entity_id": StringType(),
    "continuing_care_retirement_community": StringType(),
    "special_focus_status": StringType(),
    "abuse_icon": StringType(),
    "most_recent_health_inspection_more_than_2_years_ago": StringType(),
    "provider_changed_ownership_in_last_12_months": StringType(),
    "with_a_resident_and_family_council": StringType(),
    "automatic_sprinkler_systems_in_all_required_areas": StringType(),
    "overall_rating": StringType(),
    "overall_rating_footnote": StringType(),
    "health_inspection_rating": StringType(),
    "health_inspection_rating_footnote": StringType(),
    "qm_rating": StringType(),
    "qm_rating_footnote": StringType(),
    "long_stay_qm_rating": StringType(),
    "long_stay_qm_rating_footnote": StringType(),
    "short_stay_qm_rating": StringType(),
    "short_stay_qm_rating_footnote": StringType(),
    "staffing_rating": StringType(),
    "staffing_rating_footnote": StringType(),
    "reported_staffing_footnote": StringType(),
    "physical_therapist_staffing_footnote": StringType(),
    "reported_nurse_aide_staffing_hours_per_resident_per_day": DecimalType(9, 7),
    "reported_lpn_staffing_hours_per_resident_per_day": DecimalType(9, 7),
    "reported_rn_staffing_hours_per_resident_per_day": DecimalType(9, 7),
    "reported_licensed_staffing_hours_per_resident_per_day": DecimalType(9, 7),
    "reported_total_nurse_staffing_hours_per_resident_per_day": DecimalType(9, 7),
    "total_number_of_nurse_staff_hours_per_resident_per_day_on_the_weekend": DecimalType(9, 7),
    "registered_nurse_hours_per_resident_per_day_on_the_weekend": DecimalType(9, 7),
    "reported_physical_therapist_staffing_hours_per_resident_per_day": DecimalType(9, 7),
    "total_nursing_staff_turnover": DecimalType(5, 2),
    "total_nursing_staff_turnover_footnote": StringType(),
    "registered_nurse_turnover": DecimalType(5, 2),
    "registered_nurse_turnover_footnote": StringType(),
    "number_of_administrators_who_have_left_the_nursing_home": IntegerType(),
    "administrator_turnover_footnote": StringType(),
    "nursing_case_mix_index": DecimalType(9, 7),
    "nursing_case_mix_index_ratio": DecimalType(9, 7),
    "case_mix_nurse_aide_staffing_hours_per_resident_per_day": DecimalType(9, 7),
    "case_mix_lpn_staffing_hours_per_resident_per_day": DecimalType(9, 7),
    "case_mix_rn_staffing_hours_per_resident_per_day": DecimalType(9, 7),
    "case_mix_total_nurse_staffing_hours_per_resident_per_day": DecimalType(9, 7),
    "case_mix_weekend_total_nurse_staffing_hours_per_resident_per_day": DecimalType(9, 7),
    "adjusted_nurse_aide_staffing_hours_per_resident_per_day": DecimalType(9, 7),
    "adjusted_lpn_staffing_hours_per_resident_per_day": DecimalType(9, 7),
    "adjusted_rn_staffing_hours_per_resident_per_day": DecimalType(9, 7),
    "adjusted_total_nurse_staffing_hours_per_resident_per_day": DecimalType(9, 7),
    "adjusted_weekend_total_nurse_staffing_hours_per_resident_per_day": DecimalType(9, 7),
    "rating_cycle_1_standard_survey_health_date": StringType(),
    "rating_cycle_1_total_number_of_health_deficiencies": IntegerType(),
    "rating_cycle_1_number_of_standard_health_deficiencies": IntegerType(),
    "rating_cycle_1_number_of_complaint_health_deficiencies": IntegerType(),
    "rating_cycle_1_health_deficiency_score": IntegerType(),
    "rating_cycle_1_number_of_health_revisits": IntegerType(),
    "rating_cycle_1_health_revisit_score": IntegerType(),
    "rating_cycle_1_total_health_score": IntegerType(),
    "rating_cycle_2_standard_health_survey_date": StringType(),
    "rating_cycle_2_total_number_of_health_deficiencies": IntegerType(),
    "rating_cycle_2_number_of_standard_health_deficiencies": IntegerType(),
    "rating_cycle_2_number_of_complaint_health_deficiencies": IntegerType(),
    "rating_cycle_2_health_deficiency_score": IntegerType(),
    "rating_cycle_2_number_of_health_revisits": IntegerType(),
    "rating_cycle_2_health_revisit_score": IntegerType(),
    "rating_cycle_2_total_health_score": IntegerType(),
    "rating_cycle_3_standard_health_survey_date": StringType(),
    "rating_cycle_3_total_number_of_health_deficiencies": IntegerType(),
    "rating_cycle_3_number_of_standard_health_deficiencies": IntegerType(),
    "rating_cycle_3_number_of_complaint_health_deficiencies": IntegerType(),
    "rating_cycle_3_health_deficiency_score": IntegerType(),
    "rating_cycle_3_number_of_health_revisits": IntegerType(),
    "rating_cycle_3_health_revisit_score": IntegerType(),
    "rating_cycle_3_total_health_score": IntegerType(),
    "total_weighted_health_survey_score": DecimalType(8, 4),
    "number_of_facility_reported_incidents": IntegerType(),
    "number_of_substantiated_complaints": IntegerType(),
    "number_of_citations_from_infection_control_inspections": IntegerType(),
    "number_of_fines": IntegerType(),
    "total_amount_of_fines_in_dollars": DecimalType(8, 2),
    "number_of_payment_denials": IntegerType(),
    "total_number_of_penalties": IntegerType(),
    "location": StringType(),
    "latitude": DecimalType(8, 5),
    "longitude": DecimalType(8, 5),
    "geocoding_footnote": StringType(),
    "processing_date": StringType()
}

# -------------------------------------------------------------------
# Load manifest
# -------------------------------------------------------------------
manifest = load_manifest(bucket_name, manifest_key)
files = manifest["files"]

staffing_file = None
provider_file = None

for file_record in files:
    if file_record["dataset"] == "staffing_hours":
        staffing_file = file_record
    elif file_record["dataset"] == "provider_reference":
        provider_file = file_record

if staffing_file is None:
    raise ValueError("Could not find staffing_hours file in manifest.")

if provider_file is None:
    raise ValueError("Could not find provider_reference file in manifest.")

# -------------------------------------------------------------------
# Read raw CSVs
# -------------------------------------------------------------------
staffing_path = staffing_file["s3_path"]
provider_path = provider_file["s3_path"]

staffing_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", False)
    .csv(staffing_path)
)

provider_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", False)
    .csv(provider_path)
)

# -------------------------------------------------------------------
# Standardize columns
# -------------------------------------------------------------------
staffing_df = standardize_columns(staffing_df)
provider_df = standardize_columns(provider_df)

# -------------------------------------------------------------------
# Normalize blanks to nulls
# -------------------------------------------------------------------
staffing_df = normalize_blank_strings(staffing_df)
provider_df = normalize_blank_strings(provider_df)

# -------------------------------------------------------------------
# Explicitly cast staffing columns
# -------------------------------------------------------------------
staffing_df = cast_columns(staffing_df, staffing_column_type_map, "staffing_df")

# -------------------------------------------------------------------
# Explicitly cast provider columns
# -------------------------------------------------------------------
provider_df = cast_columns(provider_df, provider_column_type_map, "provider_df")

# -------------------------------------------------------------------
# Add lineage columns
# -------------------------------------------------------------------
staffing_df = add_lineage_columns(
    staffing_df,
    dataset_name="staffing_hours",
    source_file_name=staffing_file["file_name"]
)

provider_df = add_lineage_columns(
    provider_df,
    dataset_name="provider_reference",
    source_file_name=provider_file["file_name"]
)

# -------------------------------------------------------------------
# Write standardized outputs
# -------------------------------------------------------------------
staffing_output_path = f"s3://{bucket_name}/refined/nurse_staffing/staffing_hours/run_id={run_id}/"
provider_output_path = f"s3://{bucket_name}/refined/nurse_staffing/provider_reference/run_id={run_id}/"

(
    staffing_df.write
    .mode("overwrite")
    .parquet(staffing_output_path)
)

(
    provider_df.write
    .mode("overwrite")
    .parquet(provider_output_path)
)

job.commit()