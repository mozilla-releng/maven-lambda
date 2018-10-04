
import boto3
import distutils
import hashlib
import json
import urllib.parse

from datetime import datetime
from distutils.version import StrictVersion
from functools import reduce
from xml.etree import cElementTree as ET

print('Loading function')

# s3 = boto3.client('s3')
s3 = boto3.resource('s3')

METADATA_BASE_FILE_NAME = 'maven-metadata.xml'

def lambda_handler(event, context):
    #print("Received event: " + json.dumps(event, indent=2))

    # Get the object from the event and show its content type
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    bucket = s3.Bucket(bucket_name)
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    try:
        folder = get_folder_key(key)
        folder_content_keys = list_content_in_subfolders(bucket, folder)
        metadata = generate_release_maven_metadata(folder_content_keys)
        upload_s3_file(bucket_name, folder, METADATA_BASE_FILE_NAME, metadata, content_type='text/xml')

        checksums = generate_checksums(metadata)
        print(checksums)
        for type_, sum_ in checksums.items():
            upload_s3_file(bucket_name, folder, '{}.{}'.format(METADATA_BASE_FILE_NAME, type_), sum_)

    except Exception as e:
        print(e)
        raise e


def get_folder_key(key):
    return '/'.join(key.split('/')[:-2])


def list_content_in_subfolders(bucket, folder_key):
    return [
        file.key for file in bucket.objects.all()
        if file.key.startswith(folder_key) and '/' in file.key.strip('{}/'.format(folder_key))
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
    return s3.Object(bucket_name, key).put(Body=data, ContentEncoding='utf8', ContentType=content_type)

def generate_checksums(data):
    return {
        'md5': hashlib.md5(data).hexdigest(),
        'sha1': hashlib.sha1(data).hexdigest(),
    }
    
