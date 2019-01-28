import pytest

from datetime import datetime
from freezegun import freeze_time
from unittest.mock import MagicMock, call

from maven_lambda.metadata import (
    craft_and_upload_maven_metadata,
    generate_checksums,
    generate_last_updated,
    generate_release_maven_metadata,
    generate_snapshot_listing_metadata,
    generate_versions,
    get_artifact_id,
    get_artifact_folder,
    get_group_id,
    get_latest_version,
    get_snapshots_metadata,
    get_version,
    get_version_folder,
    is_snapshot,
    invalidate_cloudfront,
    lambda_handler,
    list_pom_files_in_subfolders,
    upload_s3_file,
    _fetch_extension_from_pom_file_content,
)


def test_lambda_handler(monkeypatch):
    event = {
        'Records': [{
            's3': {
                'bucket': {
                    'name': 'some_bucket_name',
                },
                'object': {
                    'key': 'maven2/org/mozilla/geckoview/geckoview-nightly-x86/63.0.20180830111743/geckoview-nightly-x86-63.0.20180830111743.pom',
                },
            },
        }],
    }
    context = {}
    s3_mock = MagicMock()
    monkeypatch.setattr('maven_lambda.metadata.s3', s3_mock)
    monkeypatch.setattr('maven_lambda.metadata.list_pom_files_in_subfolders', lambda _, __: [
        'maven2/org/mozilla/geckoview/geckoview-nightly-x86/63.0.20180830111743/geckoview-nightly-x86-63.0.20180830111743.pom',
    ])
    monkeypatch.setattr('maven_lambda.metadata.craft_and_upload_maven_metadata', lambda _, __, ___, metadata_function=None: None)

    lambda_handler(event, context)
    s3_mock.Bucket.assert_called_once_with('some_bucket_name')

    def fail(_, __):
        raise ConnectionError()
    monkeypatch.setattr('maven_lambda.metadata.list_pom_files_in_subfolders', fail)
    with pytest.raises(ConnectionError):
        lambda_handler(event, context)


@pytest.mark.parametrize('key, expected', ((
    'maven2/org/mozilla/geckoview/geckoview-nightly-x86/63.0.20180830111743/geckoview-nightly-x86-63.0.20180830111743.pom',
    'maven2/org/mozilla/geckoview/geckoview-nightly-x86/',
), (
    'maven2/org/mozilla/components/browser-domains/0.30.0-SNAPSHOT/browser-domains-0.30.0-20181029.154529-1.pom',
    'maven2/org/mozilla/components/browser-domains/',
), (
    'maven2/org/mozilla/components/browser-domains/0.30.0/browser-domains-0.30.0.pom',
    'maven2/org/mozilla/components/browser-domains/',
)))
def test_get_artifact_folder(key, expected):
    assert get_artifact_folder(key) == expected


@pytest.mark.parametrize('key, expected', ((
    'maven2/org/mozilla/geckoview/geckoview-nightly-x86/63.0.20180830111743/geckoview-nightly-x86-63.0.20180830111743.pom',
    'maven2/org/mozilla/geckoview/geckoview-nightly-x86/63.0.20180830111743/',
), (
    'maven2/org/mozilla/components/browser-domains/0.30.0-SNAPSHOT/browser-domains-0.30.0-20181029.154529-1.pom',
    'maven2/org/mozilla/components/browser-domains/0.30.0-SNAPSHOT/',
), (
    'maven2/org/mozilla/components/browser-domains/0.30.0/browser-domains-0.30.0.pom',
    'maven2/org/mozilla/components/browser-domains/0.30.0/',
)))
def test_get_version_folder(key, expected):
    assert get_version_folder(key) == expected


def test_list_pom_files_in_subfolders():
    bucket_mock = MagicMock()
    keys = [
        'maven2/org/mozilla/geckoview/geckoview-nightly-x86/63.0.20180830111743/geckoview-nightly-x86-63.0.20180830111743.pom',
        'maven2/org/mozilla/geckoview/geckoview-nightly-x86/63.0.20180830111743/geckoview-nightly-x86-63.0.20180830111743.pom.md5',
        'maven2/org/mozilla/geckoview/geckoview-nightly-x86/63.0.20180830111743/geckoview-nightly-x86-63.0.20180830111743.pom.sha1',
        'maven2/org/mozilla/geckoview/geckoview-nightly-x86/63.0.20180830111743/geckoview-nightly-x86-63.0.20180830111743.jar',
        'maven2/org/mozilla/geckoview/geckoview-nightly-x86/63.0.20180830111743/geckoview-nightly-x86-63.0.20180830111743.jar.md5',
        'maven2/org/mozilla/geckoview/geckoview-nightly-x86/63.0.20180830111743/geckoview-nightly-x86-63.0.20180830111743.jar.sha1',
        'maven2/org/mozilla/geckoview/geckoview-nightly-x86/64.0.20181018103737/geckoview-nightly-x86-64.0.20181018103737.pom',
        'maven2/org/mozilla/geckoview/geckoview-nightly-x86/64.0.20181018103737/geckoview-nightly-x86-64.0.20181018103737.jar',
    ]
    bucket_mock.objects.filter.return_value = [MagicMock(key=key) for key in keys]

    assert list_pom_files_in_subfolders(
        bucket_mock, 'maven2/org/mozilla/geckoview/geckoview-nightly-x86'
    ) == [
        'maven2/org/mozilla/geckoview/geckoview-nightly-x86/63.0.20180830111743/geckoview-nightly-x86-63.0.20180830111743.pom',
        'maven2/org/mozilla/geckoview/geckoview-nightly-x86/64.0.20181018103737/geckoview-nightly-x86-64.0.20181018103737.pom',
    ]
    bucket_mock.objects.filter.assert_called_once_with(
        Prefix='maven2/org/mozilla/geckoview/geckoview-nightly-x86'
    )


@pytest.mark.parametrize('key', (
    'maven2/org/mozilla/geckoview/geckoview-nightly-x86/63.0.20180830111743/geckoview-nightly-x86-63.0.20180830111743.pom',
    'org/mozilla/geckoview/geckoview-nightly-x86/63.0.20180830111743/geckoview-nightly-x86-63.0.20180830111743.pom',
))
def test_get_group_id(key):
    assert get_group_id(key) == 'org.mozilla.geckoview'


def test_get_artifact_id():
    assert get_artifact_id(
        'maven2/org/mozilla/geckoview/geckoview-nightly-x86/63.0.20180830111743/geckoview-nightly-x86-63.0.20180830111743.pom',
    ) == 'geckoview-nightly-x86'


def test_get_version():
    assert get_version(
        'maven2/org/mozilla/geckoview/geckoview-nightly-x86/63.0.20180830111743/geckoview-nightly-x86-63.0.20180830111743.pom',
    ) == '63.0.20180830111743'


def test_craft_and_upload_maven_metadata(monkeypatch):
    bucket_mock = MagicMock()
    bucket_mock.name = 'some_bucket_name'   # "name" is an argument to the Mock constructor

    metadata_function_mock = MagicMock()
    metadata_function_mock.return_value = '<some>metadata-data</some>'

    upload_s3_file_mock = MagicMock()
    monkeypatch.setattr('maven_lambda.metadata.upload_s3_file', upload_s3_file_mock)

    craft_and_upload_maven_metadata(
        bucket_mock,
        'maven2/org/mozilla/geckoview/geckoview-nightly-x86/',
        [
            'maven2/org/mozilla/geckoview/geckoview-nightly-x86/63.0.20180830111743/geckoview-nightly-x86-63.0.20180830111743.pom',
            'maven2/org/mozilla/geckoview/geckoview-nightly-x86/64.0.20181018103737/geckoview-nightly-x86-64.0.20181018103737.pom',
        ],
        metadata_function_mock
    )

    metadata_function_mock.assert_called_once_with(
        'some_bucket_name',
        [
            'maven2/org/mozilla/geckoview/geckoview-nightly-x86/63.0.20180830111743/geckoview-nightly-x86-63.0.20180830111743.pom',
            'maven2/org/mozilla/geckoview/geckoview-nightly-x86/64.0.20181018103737/geckoview-nightly-x86-64.0.20181018103737.pom',
        ]
    )
    assert upload_s3_file_mock.call_count == 3
    assert call(
        'some_bucket_name',
        'maven2/org/mozilla/geckoview/geckoview-nightly-x86/',
        'maven-metadata.xml',
        '<some>metadata-data</some>',
        content_type='text/xml'
    ) in upload_s3_file_mock.call_args_list
    assert call(
        'some_bucket_name',
        'maven2/org/mozilla/geckoview/geckoview-nightly-x86/',
        'maven-metadata.xml.md5',
        'e20eeb5c6377688eac1d5296f3ce7dcc'
    ) in upload_s3_file_mock.call_args_list
    assert call(
        'some_bucket_name',
        'maven2/org/mozilla/geckoview/geckoview-nightly-x86/',
        'maven-metadata.xml.sha1',
        '72ab62c86d47363ceb9ec2e4079e5cbfd221e3d7'
    ) in upload_s3_file_mock.call_args_list


@freeze_time('2018-10-29 16:00:30')
def test_generate_release_maven_metadata():
    assert generate_release_maven_metadata('some_bucket_name', [
        'maven2/org/mozilla/geckoview/geckoview-nightly-x86/63.0.20180830111743/geckoview-nightly-x86-63.0.20180830111743.pom',
        'maven2/org/mozilla/geckoview/geckoview-nightly-x86/64.0.20181018103737/geckoview-nightly-x86-64.0.20181018103737.pom',
        'maven2/org/mozilla/geckoview/geckoview-nightly-x86/63.0.20180830111743/geckoview-nightly-x86-63.0.20180830111743.pom',
        'maven2/org/mozilla/geckoview/geckoview-nightly-x86/65.0.20181028102554/geckoview-nightly-x86-65.0.20181028102554.pom',
        'maven2/org/mozilla/geckoview/geckoview-nightly-x86/63.0.20180830100125/geckoview-nightly-x86-63.0.20180830100125.pom',
        'maven2/org/mozilla/geckoview/geckoview-nightly-x86/65.0.20181029100346/geckoview-nightly-x86-65.0.20181029100346.pom',
        'maven2/org/mozilla/geckoview/geckoview-nightly-x86/64.0.20181019100100/geckoview-nightly-x86-64.0.20181019100100.pom',
    ]) == ("<?xml version='1.0' encoding='UTF-8'?>\n"
"<metadata>"
    "<groupId>org.mozilla.geckoview</groupId>"
    "<artifactId>geckoview-nightly-x86</artifactId>"
    "<versioning>"
        "<latest>65.0.20181029100346</latest>"
        "<release>65.0.20181029100346</release>"
        "<versions>"
            "<version>63.0.20180830100125</version>"
            "<version>63.0.20180830111743</version>"
            "<version>64.0.20181018103737</version>"
            "<version>64.0.20181019100100</version>"
            "<version>65.0.20181028102554</version>"
            "<version>65.0.20181029100346</version>"
        "</versions>"
        "<lastUpdated>20181029160030</lastUpdated>"
    "</versioning>"
"</metadata>")


@freeze_time('2018-10-31 16:00:30')
def test_generate_snapshot_listing_metadata(monkeypatch):
    monkeypatch.setattr('maven_lambda.metadata.get_snapshots_metadata', lambda _, __: [{
        'build_number': 1,
        'extension': 'aar',
        'timestamp': datetime(2018, 10, 29, 15, 45, 29),
        'version': '0.30.0',
    }, {
        'build_number': 2,
        'extension': 'aar',
        'timestamp': datetime(2018, 10, 30, 16, 46, 30),
        'version': '0.30.0',
    }])
    assert generate_snapshot_listing_metadata('some_bucket_name', [
        'maven2/org/mozilla/components/browser-domains/0.30.0-SNAPSHOT/browser-domains-0.30.0-20181030.164630-2.pom',
        'maven2/org/mozilla/components/browser-domains/0.30.0-SNAPSHOT/browser-domains-0.30.0-20181029.154529-1.pom',
    ]) == ("<?xml version='1.0' encoding='UTF-8'?>\n"
"<metadata>"
  "<groupId>org.mozilla.components</groupId>"
  "<artifactId>browser-domains</artifactId>"
  "<version>0.30.0-SNAPSHOT</version>"
  "<versioning>"
    "<snapshot>"
      "<timestamp>20181030.164630</timestamp>"
      "<buildNumber>2</buildNumber>"
    "</snapshot>"
    "<lastUpdated>20181031160030</lastUpdated>"
    "<snapshotVersions>"
      "<snapshotVersion>"
        "<extension>aar</extension>"
        "<value>0.30.0-20181030.164630-2</value>"
        "<updated>20181030164630</updated>"
      "</snapshotVersion>"
      "<snapshotVersion>"
        "<extension>aar</extension>"
        "<value>0.30.0-20181029.154529-1</value>"
        "<updated>20181029154529</updated>"
      "</snapshotVersion>"
    "</snapshotVersions>"
  "</versioning>"
"</metadata>")


def test_get_snapshots_metadata(monkeypatch):
    monkeypatch.setattr('maven_lambda.metadata._fetch_extension_from_pom_file_content', lambda _, __: 'aar')
    assert get_snapshots_metadata('some_bucket_name', [
        'maven2/org/mozilla/components/browser-domains/0.30.0-SNAPSHOT/browser-domains-0.30.0-20181030.164630-2.pom',
        'maven2/org/mozilla/components/browser-domains/0.30.0-SNAPSHOT/browser-domains-0.30.0-20181029.154529-1.pom',
    ]) == [{
        'build_number': 2,
        'extension': 'aar',
        'timestamp': datetime(2018, 10, 30, 16, 46, 30),
        'version': '0.30.0',
    }, {
        'build_number': 1,
        'extension': 'aar',
        'timestamp': datetime(2018, 10, 29, 15, 45, 29),
        'version': '0.30.0',
    }]


def test_fetch_extension_from_pom_file_content(monkeypatch):
    s3_mock = MagicMock()
    bucket_mock = MagicMock()
    s3_mock.Bucket.return_value = bucket_mock

    def fake_download(_, destination):
        with open(destination, 'w') as f:
            f.write('''<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0">
  <packaging>aar</packaging>
</project>''')

    bucket_mock.download_file.side_effect = fake_download

    monkeypatch.setattr('maven_lambda.metadata.s3', s3_mock)

    assert _fetch_extension_from_pom_file_content('some_bucket_name',
        'maven2/org/mozilla/components/browser-domains/0.30.0-SNAPSHOT/browser-domains-0.30.0-20181030.164630-2.pom',
    ) == 'aar'

    s3_mock.Bucket.assert_called_once_with('some_bucket_name')


def test_generate_versions():
    assert generate_versions([
        'maven2/org/mozilla/geckoview/geckoview-nightly-x86/63.0.20180830111743/geckoview-nightly-x86-63.0.20180830111743.pom',
        'maven2/org/mozilla/geckoview/geckoview-nightly-x86/64.0.20181018103737/geckoview-nightly-x86-64.0.20181018103737.pom',
        'maven2/org/mozilla/geckoview/geckoview-nightly-x86/63.0.20180830111743/geckoview-nightly-x86-63.0.20180830111743.pom',
        'maven2/org/mozilla/geckoview/geckoview-nightly-x86/65.0.20181028102554/geckoview-nightly-x86-65.0.20181028102554.pom',
        'maven2/org/mozilla/geckoview/geckoview-nightly-x86/63.0.20180830100125/geckoview-nightly-x86-63.0.20180830100125.pom',
        'maven2/org/mozilla/geckoview/geckoview-nightly-x86/65.0.20181029100346/geckoview-nightly-x86-65.0.20181029100346.pom',
        'maven2/org/mozilla/geckoview/geckoview-nightly-x86/64.0.20181019100100/geckoview-nightly-x86-64.0.20181019100100.pom',
    ]) == [
        '63.0.20180830100125',
        '63.0.20180830111743',
        '64.0.20181018103737',
        '64.0.20181019100100',
        '65.0.20181028102554',
        '65.0.20181029100346',
    ]


@freeze_time('2018-10-29 16:00:30')
def test_generate_last_updated():
    assert generate_last_updated() == '20181029160030'


@pytest.mark.parametrize('versions, exclude_snapshots, expected', ((
    (
        '64.0.20181018103737',
        '63.0.20180830111743',
        '65.0.20181028102554',
        '63.0.20180830100125',
        '65.0.20181029100346',
        '64.0.20181019100100',
    ),
    False,
    '65.0.20181029100346',
), (
    ('64.0', '64.0-SNAPSHOT'),
    False,
    '64.0',
), (
    ('64.0', '64.0-SNAPSHOT'),
    True,
    '64.0',
), (
    ('64.0', '64.0-SNAPSHOT', '65.0'),
    False,
    '65.0',
), (
    ('63.0-SNAPSHOT', '64.0-SNAPSHOT', '65.0-SNAPSHOT'),
    False,
    '65.0-SNAPSHOT',
), (
    ('63.0-SNAPSHOT', '64.0-SNAPSHOT', '65.0-SNAPSHOT'),
    True,
    None,
)))
def test_get_latest_version(versions, exclude_snapshots, expected):
    assert get_latest_version(versions, exclude_snapshots) == expected


def test_upload_s3_file(monkeypatch):
    s3_mock = MagicMock()
    object_mock = MagicMock()
    s3_mock.Object.return_value = object_mock
    invalidate_cloudfront_mock = MagicMock()
    monkeypatch.setattr('maven_lambda.metadata.s3', s3_mock)
    monkeypatch.setattr('maven_lambda.metadata.invalidate_cloudfront', invalidate_cloudfront_mock)
    upload_s3_file(
        'some_bucket', 'some/folder/', 'some_file', 'some data', content_type='some/content-type'
    )

    s3_mock.Object.assert_called_once_with('some_bucket', 'some/folder/some_file')
    object_mock.put.assert_called_once_with(Body='some data', ContentType='some/content-type')
    invalidate_cloudfront_mock.assert_called_once_with(path='some/folder/some_file')


@freeze_time('2018-10-29 16:00:30')
@pytest.mark.parametrize('cloudfront_distribution_id', (None, 'some-id'))
def test_invalidate_cloudfront(monkeypatch, cloudfront_distribution_id):
    cloudfront_mock = MagicMock()
    monkeypatch.setattr('maven_lambda.metadata.cloudfront', cloudfront_mock)
    monkeypatch.setattr('os.environ.get', lambda _, __: cloudfront_distribution_id)
    invalidate_cloudfront('some/folder/some_file')

    if cloudfront_distribution_id:
        cloudfront_mock.create_invalidation.assert_called_once_with(
            DistributionId='some-id',
            InvalidationBatch={
                'Paths': {
                    'Quantity': 1,
                    'Items': [
                        'some/folder/some_file',
                    ],
                },
                'CallerReference': str('1540828830'),
            }
        )
    else:
        cloudfront_mock.create_invalidation.assert_not_called()


@pytest.mark.parametrize('data', (
    'known string',
    b'known string',
))
def test_generate_checksums(data):
    assert generate_checksums(data) == {
        'md5': 'a48fba03a9ac529b358935164826d9fe',
        'sha1': '714f4de20aa1899ed09e22a82304e12d4658eac1',
    }


@pytest.mark.parametrize('key, expected', (
    ('maven2/org/mozilla/components/browser-domains/0.30.0-SNAPSHOT/', True),
    ('maven2/org/mozilla/components/browser-domains/0.30.0-SNAPSHOT/browser-domains-0.30.0-20181029.154529-1.pom', True),
    ('maven2/org/mozilla/components/browser-domains/0.30.0/browser-domains-0.30.0.pom', False)
))
def test_is_snapshot(key, expected):
    assert is_snapshot(key) == expected
