
import boto3
import hashlib
import io
import os
import slugid
import tempfile
import urllib.parse

from datetime import datetime
from functools import reduce
from mozilla_version.maven import MavenVersion
from mozilla_version.errors import PatternNotMatchedError
from xml.etree import cElementTree as ET


# logging doesn't work on AWS Lambda, at first
print('Loading function')

s3 = boto3.resource('s3')
cloudfront = boto3.client('cloudfront')

METADATA_BASE_FILE_NAME = 'maven-metadata.xml'

ET.register_namespace('', 'http://maven.apache.org/POM/4.0.0')
XML_NAMESPACES = {
    'm': 'http://maven.apache.org/POM/4.0.0',
}

POM_TIMESTAMP = '%Y%m%d%H%M%S'
SNAPSHOT_FILE_TIMESTAMP = '%Y%m%d.%H%M%S'


def lambda_handler(event, context):
    print('Processing a new event...')
    print('Event: {}. Context: {}'.format(event, context))

    bucket_name = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    print('Processing key "{}" from bucket "{}"...'.format(key, bucket_name))

    bucket = s3.Bucket(bucket_name)

    try:
        artifact_folder = get_artifact_folder(key)
        print('Extracted artifact folder "{}" from key'.format(artifact_folder))
        poms_in_artifact_folder = list_pom_files_in_subfolders(bucket, artifact_folder)
        print('Found .pom in artifact folder (and subfolders): {}'.format(poms_in_artifact_folder))
        craft_and_upload_maven_metadata(
            bucket, artifact_folder, poms_in_artifact_folder,
            metadata_function=generate_release_maven_metadata
        )

        if is_snapshot(key):
            version_folder = get_version_folder(key)
            print('Extracted version folder "{}" from key'.format(version_folder))
            poms_in_version_folder = [
                key for key in poms_in_artifact_folder
                if key.startswith(version_folder)
            ]
            print('.pom in version folder: {}'.format(poms_in_version_folder))
            craft_and_upload_maven_metadata(
                bucket, version_folder, poms_in_version_folder,
                metadata_function=generate_snapshot_listing_metadata
            )

    except Exception as e:
        print(e)
        raise

    print('Done processing folder "{}"'.format(artifact_folder))


def get_artifact_folder(key):
    return _take_items_out_of_path(key, number_of_items=2)


def get_version_folder(key):
    return _take_items_out_of_path(key, number_of_items=1)


def _take_items_out_of_path(key, number_of_items):
    split_path_without_last_two_items = key.split('/')[:-number_of_items]
    folder = '/'.join(split_path_without_last_two_items)
    folder_with_trailing_slash = '{}/'.format(folder)
    return folder_with_trailing_slash


def list_pom_files_in_subfolders(bucket, folder_key):
    return [
        file.key for file in bucket.objects.filter(Prefix=folder_key)
        if file.key.endswith('.pom')
    ]


def get_group_id(key):
    folder_names_but_last_three = key.split('/')[:-3]
    if folder_names_but_last_three[0] in ('maven', 'maven2'):
        # artifacts are usually uploaded under maven2/ which is not part of the groupId
        folder_names_without_maven_prefix = folder_names_but_last_three[1:]
    else:
        folder_names_without_maven_prefix = folder_names_but_last_three
    return '.'.join(folder_names_without_maven_prefix)


def get_artifact_id(key):
    return key.split('/')[-3]


def get_version(key):
    return key.split('/')[-2]


def craft_and_upload_maven_metadata(bucket, folder, pom_files, metadata_function):
    bucket_name = bucket.name
    metadata = metadata_function(bucket_name, pom_files)
    print('Generated maven-metadata content: {}'.format(metadata))
    upload_s3_file(
        bucket_name, folder, METADATA_BASE_FILE_NAME, metadata, content_type='text/xml'
    )
    print('Uploaded new maven-metadata.xml')

    checksums = generate_checksums(metadata)
    print('New maven-metadata.xml checksums: {}'.format(checksums))
    for type_, sum_ in checksums.items():
        upload_s3_file(
            bucket_name, folder, '{}.{}'.format(METADATA_BASE_FILE_NAME, type_), sum_
        )
        print('Uploaded new {} checksum file'.format(type_))


def generate_release_maven_metadata(_, folder_content_keys):
    versions_per_path = generate_versions(folder_content_keys)
    latest_version = get_latest_version(versions_per_path, exclude_snapshots=False)
    latest_non_snapshot_version = get_latest_version(versions_per_path, exclude_snapshots=True)

    root = _generate_xml_root_of_common_maven_metadata(folder_content_keys)

    versioning = ET.SubElement(root, 'versioning')

    ET.SubElement(versioning, 'latest').text = latest_version
    ET.SubElement(versioning, 'release').text = '' if latest_non_snapshot_version is None \
        else latest_non_snapshot_version

    versions = ET.SubElement(versioning, 'versions')

    for version in sorted(set(versions_per_path.values())):
        ET.SubElement(versions, 'version').text = version

    ET.SubElement(versioning, 'lastUpdated').text = generate_last_updated()

    return _convert_xml_root_to_string(root)


def _generate_xml_root_of_common_maven_metadata(folder_content_keys):
    first_listed_version_key = folder_content_keys[0]

    root = ET.Element('metadata')
    ET.SubElement(root, 'groupId').text = get_group_id(first_listed_version_key)
    ET.SubElement(root, 'artifactId').text = get_artifact_id(first_listed_version_key)

    return root


def _convert_xml_root_to_string(root):
    # XXX ET.tostring() strips the xml_declaration out if using encoding='unicode'
    stream = io.StringIO()
    ET.ElementTree(root).write(
        stream, encoding='unicode', xml_declaration=True, method='xml', short_empty_elements=False
    )
    return stream.getvalue()


def generate_snapshot_listing_metadata(bucket_name, pom_files_keys):
    snapshots_metadata = get_snapshots_metadata(bucket_name, pom_files_keys)
    sorted_snapshot_metadata = sorted(
        snapshots_metadata, key=lambda x: x['timestamp'], reverse=True
    )
    latest_snapshot_metadata = sorted_snapshot_metadata[0]

    root = _generate_xml_root_of_common_maven_metadata(pom_files_keys)
    ET.SubElement(root, 'version').text = get_version(pom_files_keys[0])

    versioning = ET.SubElement(root, 'versioning')
    snapshot = ET.SubElement(versioning, 'snapshot')
    ET.SubElement(snapshot, 'timestamp').text = latest_snapshot_metadata['timestamp'].strftime(
        SNAPSHOT_FILE_TIMESTAMP
    )
    ET.SubElement(snapshot, 'buildNumber').text = str(latest_snapshot_metadata['build_number'])

    ET.SubElement(versioning, 'lastUpdated').text = generate_last_updated()

    snapshot_versions = ET.SubElement(versioning, 'snapshotVersions')

    for metadata in sorted_snapshot_metadata:
        snapshot_version = ET.SubElement(snapshot_versions, 'snapshotVersion')
        ET.SubElement(snapshot_version, 'extension').text = metadata['extension']
        ET.SubElement(snapshot_version, 'value').text = '{}-{}-{}'.format(
            metadata['version'],
            metadata['timestamp'].strftime(SNAPSHOT_FILE_TIMESTAMP),
            metadata['build_number']
        )
        ET.SubElement(snapshot_version, 'updated').text = metadata['timestamp'].strftime(
            POM_TIMESTAMP
        )

    return _convert_xml_root_to_string(root)


def get_snapshots_metadata(bucket_name, all_snapshot_pom_keys):
    pom_keys_and_file_names = [
        (key, key.split('/')[-1])
        for key in all_snapshot_pom_keys
    ]

    return [{
        'build_number': _extract_build_number_from_file_name(file_name),
        'extension': _fetch_extension_from_pom_file_content(bucket_name, key),
        'timestamp': _extract_timestamp_from_file_name(file_name),
        'version': _extract_version_from_file_name(file_name),
    } for key, file_name in pom_keys_and_file_names]


def _extract_build_number_from_file_name(file_name):
    file_name_without_extension = file_name.rstrip('.pom')
    build_number = file_name_without_extension.split('-')[-1]
    return int(build_number)


def _fetch_extension_from_pom_file_content(bucket_name, pom_key):
    with tempfile.TemporaryDirectory() as d:
        temporary_pom_path = os.path.join(d, 'pom.xml')
        s3.Bucket(bucket_name).download_file(pom_key, temporary_pom_path)
        tree = ET.parse(temporary_pom_path)

    root = tree.getroot()
    packaging_element = root.find('m:packaging', XML_NAMESPACES)
    return packaging_element.text


def _extract_timestamp_from_file_name(file_name):
    timestamp = file_name.split('-')[-2]
    return datetime.strptime(timestamp, SNAPSHOT_FILE_TIMESTAMP)


def _extract_version_from_file_name(file_name):
    return file_name.split('-')[-3]


def generate_versions(folder_content_keys):
    return {
        key: get_version(key) for key in folder_content_keys if key
    }


def generate_last_updated():
    return datetime.utcnow().strftime(POM_TIMESTAMP)


def get_latest_version(versions_per_path, exclude_snapshots=False):
    maven_versions = []
    for path, version in versions_per_path.items():
        try:
            maven_versions.append(MavenVersion.parse(version))
        except PatternNotMatchedError as error:
            raise ValueError(
                '"{}" does not contain a valid version. See root error.'.format(path)
            ) from error

    if exclude_snapshots:
        maven_versions = [version for version in maven_versions if not version.is_snapshot]
    if not maven_versions:
        return None
    latest_version = reduce(lambda x, y: x if x >= y else y, maven_versions)
    return str(latest_version)


def upload_s3_file(bucket_name, folder, file_name, data, content_type='text/plain'):
    folder = folder.rstrip('/')
    key = '{}/{}'.format(folder, file_name)
    s3.Object(bucket_name, key).put(Body=data, ContentType=content_type)
    invalidate_cloudfront(path=key)


def invalidate_cloudfront(path):
    distribution_id = os.environ.get('CLOUDFRONT_DISTRIBUTION_ID', None)

    if distribution_id:
        path = path if path.startswith('/') else '/{}'.format(path)
        request_id = slugid.nice().decode('utf-8')  # nice() returns bytes by default

        # TODO retry this call
        cloudfront.create_invalidation(
            DistributionId=distribution_id,
            InvalidationBatch={
                'Paths': {
                    'Quantity': 1,
                    'Items': [
                        path,
                    ],
                },
                'CallerReference': request_id,
            }
        )


def generate_checksums(data):
    if isinstance(data, str):
        data = data.encode()
    return {
        'md5': hashlib.md5(data).hexdigest(),
        'sha1': hashlib.sha1(data).hexdigest(),
    }


def is_snapshot(key):
    return '-SNAPSHOT' in key
