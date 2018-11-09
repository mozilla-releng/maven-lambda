
import boto3
import hashlib
import io
import urllib.parse

from datetime import datetime
from functools import reduce
from mozilla_version.maven import MavenVersion
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
        artifact_folder = get_artifact_folder(key)
        print('Extracted artifact folder "{}" from key'.format(artifact_folder))
        poms_in_artifact_folder = list_pom_files_in_subfolders(bucket, artifact_folder)
        print('Found .pom in artifact folder (and subfolders): {}'.format(poms_in_artifact_folder))
        lol(bucket, artifact_folder, poms_in_artifact_folder, generate_release_maven_metadata)

        if is_snapshot(key):
            version_folder = get_version_folder(key)
            print('Extracted version folder "{}" from key'.format(version_folder))
            poms_in_version_folder = [
                key for key in poms_in_artifact_folder
                if key.startswith(version_folder)
            ]
            print('.pom in version folder: {}'.format(poms_in_version_folder))
            lol(bucket, version_folder, poms_in_version_folder, generate_snapshot_listing_metadata)

    except Exception as e:
        print(e)
        raise

    print('Done processing folder "{}"'.format(artifact_folder))


def get_artifact_folder(key):
    return take_items_out_of_path(key, number_of_items=2)


def get_version_folder(key):
    return take_items_out_of_path(key, number_of_items=1)


def take_items_out_of_path(key, number_of_items):
    split_path_without_last_two_items = key.split('/')[:-number_of_items]
    folder = '/'.join(split_path_without_last_two_items)
    folder_with_trailing_slash = '{}/'.format(folder)
    return folder_with_trailing_slash


def list_pom_files_in_subfolders(bucket, folder_key):
    return [
        file.key for file in bucket.objects.filter(Prefix=folder_key)
        if file.key.endswith('.pom')
    ]


def lol(bucket, folder, pom_files, metadata_function):
    metadata = metadata_function(pom_files)
    print('Generated maven-metadata content: {}'.format(metadata))
    upload_s3_file(
        bucket.name, folder, METADATA_BASE_FILE_NAME, metadata, content_type='text/xml'
    )
    print('Uploaded new maven-metadata.xml')

    checksums = generate_checksums(metadata)
    print('New maven-metadata.xml checksums: {}'.format(checksums))
    for type_, sum_ in checksums.items():
        upload_s3_file(
            bucket.name, folder, '{}.{}'.format(METADATA_BASE_FILE_NAME, type_), sum_
        )
        print('Uploaded new {} checksum file'.format(type_))


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


def generate_release_maven_metadata(folder_content_keys):
    all_versions = generate_versions(folder_content_keys)
    latest_version = get_latest_version(all_versions, exclude_snapshots=False)
    latest_non_snapshot_version = get_latest_version(all_versions, exclude_snapshots=True)

    root = _generate_xml_root_of_common_maven_metadata(folder_content_keys)

    versioning = ET.SubElement(root, 'versioning')

    ET.SubElement(versioning, 'latest').text = latest_version
    ET.SubElement(versioning, 'release').text = '' if latest_non_snapshot_version is None \
        else latest_non_snapshot_version

    versions = ET.SubElement(versioning, 'versions')

    for version in all_versions:
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


def generate_snapshot_listing_metadata(folder_content_keys):
    snapshots_metadata = get_snapshots_metadata(folder_content_keys)
    latest_snapshot_metadata = find_latest_snapshot(snapshots_metadata)

    root = _generate_xml_root_of_common_maven_metadata(folder_content_keys)
    ET.SubElement(root, 'version').text = get_version(folder_content_keys[0])

    versioning = ET.SubElement(root, 'versioning')
    snapshot = ET.SubElement(versioning, 'snapshot')
    ET.SubElement(snapshot, 'timestamp').text = latest_snapshot_metadata['timestamp']
    ET.SubElement(snapshot, 'buildNumber').text = str(latest_snapshot_metadata['build_number'])

    ET.SubElement(versioning, 'lastUpdated').text = generate_last_updated()

    snapshot_versions = ET.SubElement(versioning, 'snapshotVersions')

    for metadata in snapshots_metadata:
        snapshot_version = ET.SubElement(snapshot_versions, 'snapshotVersion')
        ET.SubElement(snapshot_version, 'extension').text = metadata['extension']
        ET.SubElement(snapshot_version, 'value').text = '{}-{}-{}'.format(
            metadata['version'], metadata['timestamp'], metadata['build_number']
        )
        ET.SubElement(snapshot_version, 'updated').text = ''.join(metadata['timestamp'].split('.'))

    return _convert_xml_root_to_string(root)


def get_snapshots_metadata(all_snapshots):
    # TODO: Do not hardcode anymore
    return [{
        'build_number': 1,
        'extension': 'aar',
        'timestamp': '20181029.154529',
        'version': '0.30.0',
    }]


def find_latest_snapshot(all_snapshots_metadata):
    return reduce(
        lambda x, y: x if x['build_number'] >= y['build_number'] else y, all_snapshots_metadata
    )


def generate_versions(folder_content_keys):
    return sorted(list(set([
        get_version(key) for key in folder_content_keys if key
    ])))


def generate_last_updated():
    return datetime.utcnow().strftime('%Y%m%d%H%M%S')


def get_latest_version(versions, exclude_snapshots=False):
    maven_versions = [MavenVersion.parse(version) for version in versions]
    if exclude_snapshots:
        maven_versions = [version for version in maven_versions if not version.is_snapshot]
    if not maven_versions:
        return None
    latest_version = reduce(lambda x, y: x if x >= y else y, maven_versions)
    return str(latest_version)


def upload_s3_file(bucket_name, folder, file_name, data, content_type='text/plain'):
    folder = folder.rstrip('/')
    key = '{}/{}'.format(folder, file_name)
    return s3.Object(bucket_name, key).put(Body=data, ContentType=content_type)


def generate_checksums(data):
    if isinstance(data, str):
        data = data.encode()
    return {
        'md5': hashlib.md5(data).hexdigest(),
        'sha1': hashlib.sha1(data).hexdigest(),
    }


def is_snapshot(key):
    return '-SNAPSHOT/' in key
