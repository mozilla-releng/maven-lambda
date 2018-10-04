
import boto3
import distutils
import hashlib
import json
import urllib.parse

from datetime import datetime
from distutils.version import StrictVersion
from functools import reduce
from xml.etree import cElementTree as ET


# logging doesn't work on AWS Lambda, at first
print('Loading function')

s3 = boto3.resource('s3')

METADATA_BASE_FILE_NAME = 'maven-metadata.xml'


def lambda_handler(event, context):
    print('Processing a new event...')
    print('Event: {}. Context: {}'.format(event, context))

    bucket_name = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    print('Processing key "{}" from bucket "{}"...'.format(key, bucket_name))

    bucket = s3.Bucket(bucket_name)

    try:
        folder = get_folder_key(key)
        print('Extracted folder "{}" from key'.format(folder))
        folder_content_keys = list_pom_files_in_subfolders(bucket, folder)
        print('Found .pom files: {}'.format(folder_content_keys))
        metadata = generate_release_maven_metadata(folder_content_keys)
        print('Generated maven-metadata content: {}'.format(metadata))
        upload_s3_file(bucket_name, folder, METADATA_BASE_FILE_NAME, metadata, content_type='text/xml')
        print('Uploaded new maven-metadata.xml')

        checksums = generate_checksums(metadata)
        print('New maven-metadata.xml checksums: {}'.format(checksums))
        for type_, sum_ in checksums.items():
            upload_s3_file(bucket_name, folder, '{}.{}'.format(METADATA_BASE_FILE_NAME, type_), sum_)
            print('Uploaded new {} checksum file'.format(type_))

    except Exception as e:
        print(e)
        raise

    print('Done processing folder "{}"'.format(folder))


def get_folder_key(key):
    split_path_without_last_two_items = key.split('/')[:-2]
    folder = '/'.join(split_path_without_last_two_items)
    folder_with_trailing_slash = '{}/'.format(folder)
    return folder_with_trailing_slash


def list_pom_files_in_subfolders(bucket, folder_key):
    return [
        file.key for file in bucket.objects.filter(Prefix=folder_key)
        if file.key.endswith('.pom')
    ]


def get_group_id(key):
    return '.'.join(key.split('/')[:-3])


def get_artifact_id(key):
    return key.split('/')[-3]


def get_version(key):
    return key.split('/')[-2]


def generate_release_maven_metadata(folder_content_keys):
    first_listed_version_key = folder_content_keys[0]
    all_versions = generate_versions(folder_content_keys)
    latest_version = get_latest_version(all_versions)

    root = ET.Element('metadata')
    ET.SubElement(root, 'groupId').text = get_group_id(first_listed_version_key)
    ET.SubElement(root, 'artifactId').text = get_artifact_id(first_listed_version_key)

    versioning = ET.SubElement(root, 'versioning')

    ET.SubElement(versioning, 'latest').text = latest_version
    ET.SubElement(versioning, 'release').text = latest_version

    versions = ET.SubElement(versioning, 'versions')

    for version in all_versions:
        ET.SubElement(versions, 'version').text = version


    ET.SubElement(versioning, 'lastUpdated').text = generate_last_updated()

    return ET.tostring(root, encoding="utf8")


def generate_versions(folder_content_keys):
    return sorted(list(set([
        get_version(key) for key in folder_content_keys
        if key
    ])))


def generate_last_updated():
    return datetime.utcnow().strftime('%Y%m%d%H%M%S')


def get_latest_version(versions):
    strict_versions = [StrictVersion(version) for version in versions]
    latest_strict_version = reduce(lambda x, y: x if x >= y else y, strict_versions)
    return str(latest_strict_version)


def upload_s3_file(bucket_name, folder, file_name, data, content_type='text/plain'):
    key = '{}/{}'.format(folder, file_name)
    return s3.Object(bucket_name, key).put(Body=data, ContentType=content_type)


def generate_checksums(data):
    return {
        'md5': hashlib.md5(data).hexdigest(),
        'sha1': hashlib.sha1(data).hexdigest(),
    }
