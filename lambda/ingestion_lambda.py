import json
import os
import logging
from datetime import datetime, timezone

import boto3
import requests
from botocore.exceptions import ClientError

# ---------------------------
# Logging Setup
# ---------------------------
logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client("s3")
secrets_client = boto3.client("secretsmanager")


# ---------------------------
# Custom Exceptions
# ---------------------------
class PipelineError(Exception):
    pass


class SecretError(PipelineError):
    pass


class GoogleAuthError(PipelineError):
    pass


class GoogleDriveError(PipelineError):
    pass


class S3UploadError(PipelineError):
    pass


# ---------------------------
# Secrets Manager
# ---------------------------
def get_secret(secret_name: str) -> dict:
    try:
        response = secrets_client.get_secret_value(SecretId=secret_name)
        secret_string = response.get("SecretString")

        if not secret_string:
            raise SecretError("SecretString is empty.")

        return json.loads(secret_string)

    except ClientError as e:
        logger.exception("Failed to retrieve secret")
        raise SecretError(f"Secrets Manager error: {str(e)}")

    except json.JSONDecodeError:
        raise SecretError("Secret is not valid JSON")


# ---------------------------
# Google Auth
# ---------------------------
def get_access_token(client_id, client_secret, refresh_token):
    try:
        response = requests.post(
            "https://oauth2.googleapis.com/token",
            data={
                "client_id": client_id,
                "client_secret": client_secret,
                "refresh_token": refresh_token,
                "grant_type": "refresh_token"
            },
            timeout=30
        )

        response.raise_for_status()
        data = response.json()

        access_token = data.get("access_token")

        if not access_token:
            raise GoogleAuthError(f"No access token returned: {data}")

        return access_token

    except requests.exceptions.RequestException as e:
        logger.exception("Google token request failed")
        raise GoogleAuthError(f"Token request failed: {str(e)}")


# ---------------------------
# Google Drive
# ---------------------------
def get_file_metadata(file_id, access_token):
    try:
        response = requests.get(
            f"https://www.googleapis.com/drive/v3/files/{file_id}",
            headers={"Authorization": f"Bearer {access_token}"},
            params={"fields": "id,name,mimeType,modifiedTime,size"},
            timeout=30
        )

        response.raise_for_status()
        return response.json()

    except requests.exceptions.RequestException as e:
        logger.exception("Metadata retrieval failed")
        raise GoogleDriveError(f"Metadata request failed: {str(e)}")


def download_file(file_id, access_token):
    try:
        response = requests.get(
            f"https://www.googleapis.com/drive/v3/files/{file_id}?alt=media",
            headers={"Authorization": f"Bearer {access_token}"},
            timeout=120
        )

        response.raise_for_status()
        return response.content

    except requests.exceptions.RequestException as e:
        logger.exception("File download failed")
        raise GoogleDriveError(f"Download failed: {str(e)}")


# ---------------------------
# S3 Upload
# ---------------------------
def upload_to_s3(bucket, key, content, content_type):
    try:
        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=content,
            ContentType=content_type
        )
    except ClientError as e:
        logger.exception("S3 upload failed")
        raise S3UploadError(f"S3 upload failed: {str(e)}")


# ---------------------------
# Utilities
# ---------------------------
def sanitize_filename(name):
    if not name:
        return "unknown.csv"

    return (
        name.strip()
        .replace(" ", "_")
        .replace("/", "_")
        .replace("\\", "_")
    )


def build_manifest(run_id, files):
    return {
        "run_id": run_id,
        "source": "google_drive",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "files": files
    }


# ---------------------------
# Lambda Handler
# ---------------------------
def lambda_handler(event, context):
    try:
        logger.info("Event received: %s", json.dumps(event))

        # Validate input
        run_id = event.get("run_id")
        bucket = event.get("bucket_name")

        if not run_id or not bucket:
            raise PipelineError("Missing run_id or bucket_name")

        raw_prefix = event.get("raw_prefix", "raw/google_drive/")
        manifest_prefix = event.get("manifest_prefix", "manifests/")

        if not raw_prefix.endswith("/"):
            raw_prefix += "/"
        if not manifest_prefix.endswith("/"):
            manifest_prefix += "/"

        # Get secret
        secret_name = os.environ.get("SECRET_NAME")
        if not secret_name:
            raise PipelineError("SECRET_NAME environment variable not set")

        secret = get_secret(secret_name)

        client_id = secret["google_client_id"]
        client_secret = secret["google_client_secret"]
        refresh_token = secret["google_refresh_token"]

        files_config = [
            ("staffing_hours", secret["nurse_staffing_hours_file_id"]),
            ("provider_reference", secret["provider_info_file_id"])
        ]

        # Authenticate
        access_token = get_access_token(client_id, client_secret, refresh_token)

        file_records = []

        for dataset_name, file_id in files_config:
            logger.info("Processing dataset: %s", dataset_name)

            metadata = get_file_metadata(file_id, access_token)
            filename = sanitize_filename(metadata.get("name"))

            if not filename.endswith(".csv"):
                filename += ".csv"

            content = download_file(file_id, access_token)

            s3_key = f"{raw_prefix}{run_id}/{filename}"

            upload_to_s3(bucket, s3_key, content, "text/csv")

            file_records.append({
                "dataset": dataset_name,
                "file_id": file_id,
                "file_name": filename,
                "s3_path": f"s3://{bucket}/{s3_key}",
                "size_bytes": len(content)
            })

            logger.info("Uploaded %s to %s", filename, s3_key)

        # Build manifest
        manifest = build_manifest(run_id, file_records)
        manifest_key = f"{manifest_prefix}{run_id}.json"

        upload_to_s3(
            bucket,
            manifest_key,
            json.dumps(manifest, indent=2).encode("utf-8"),
            "application/json"
        )

        logger.info("Manifest created: %s", manifest_key)

        return {
            "status": "SUCCESS",
            "manifest_path": f"s3://{bucket}/{manifest_key}",
            "bucket_name": bucket,
            "run_id": run_id,
            "manifest_key": manifest_key,
            "files_processed": len(file_records)
        }

    except PipelineError as e:
        logger.error("Pipeline error: %s", str(e))
        raise

    except Exception as e:
        logger.exception("Unexpected failure")
        raise