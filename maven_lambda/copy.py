import boto3
import os


class NotFound(Exception):
    pass


def lambda_handler(event, context):
    TARGET_BUCKET = os.environ["TARGET_BUCKET"]
    s3 = boto3.client("s3")
    s3_event = event["Records"][0]["s3"]
    try:
        if s3_object_has_more_than_one_version(
            s3, s3_event["bucket"]["name"], s3_event["object"]["key"]
        ):
            return {"statusCode": 409}
    except NotFound:
        return {"statusCode": 404}
    s3.copy_object(
        Bucket=TARGET_BUCKET,
        CopySource={
            "Bucket": s3_event["bucket"]["name"],
            "Key": s3_event["object"]["key"],
        },
        Key=s3_event["object"]["key"],
    )
    return {"statusCode": 200}


def s3_object_has_more_than_one_version(s3, bucket, key):
    versions = s3.list_object_versions(Bucket=bucket, Prefix=key)
    try:
        versions = versions["Versions"]
    except KeyError:
        raise NotFound()
    versions = [v for v in versions if v["Key"] == key]
    return len(versions) > 1
