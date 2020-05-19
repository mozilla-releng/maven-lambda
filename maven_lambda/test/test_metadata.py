import pytest

from botocore.exceptions import ClientError
from datetime import datetime
from freezegun import freeze_time
from unittest.mock import MagicMock, call

from maven_lambda.metadata import (
    craft_and_upload_maven_metadata,
    generate_checksums,
    generate_last_updated,
    generate_release_maven_metadata,
    generate_versions,
    get_artifact_id,
    get_artifact_folder,
    get_group_id,
    get_latest_version,
    get_version,
    get_version_folder,
    invalidate_cloudfront_cache,
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
    monkeypatch.setattr(
        'maven_lambda.metadata.craft_and_upload_maven_metadata',
        lambda _, __, ___, metadata_function=None: [
            'maven2/org/mozilla/geckoview/geckoview-nightly-x86/63.0.20180830111743/geckoview-nightly-x86-63.0.20180830111743.pom',
        ]
    )

    def cloudfront(paths):
        assert paths == ['maven2/org/mozilla/geckoview/geckoview-nightly-x86/63.0.20180830111743/geckoview-nightly-x86-63.0.20180830111743.pom']
    monkeypatch.setattr('maven_lambda.metadata.invalidate_cloudfront_cache', cloudfront)

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

    assert craft_and_upload_maven_metadata(
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


@pytest.mark.parametrize('xml_data, expected_format', ((
    '''<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0">
<packaging>aar</packaging>
</project>''',
    'aar',
), (
    '''<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0">
<packaging>jar</packaging>
</project>''',
    'jar',
), (
    '''<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0">
</project>''',
    'jar',
)))
def test_fetch_extension_from_pom_file_content(monkeypatch, xml_data, expected_format):
    s3_mock = MagicMock()
    bucket_mock = MagicMock()
    s3_mock.Bucket.return_value = bucket_mock

    def fake_download(_, destination):
        with open(destination, 'w') as f:
            f.write(xml_data)

    bucket_mock.download_file.side_effect = fake_download

    monkeypatch.setattr('maven_lambda.metadata.s3', s3_mock)

    assert _fetch_extension_from_pom_file_content('some_bucket_name',
        'maven2/org/mozilla/components/browser-domains/0.30.0-SNAPSHOT/browser-domains-0.30.0-20181030.164630-2.pom',
    ) == expected_format

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
    ]) == {
        'maven2/org/mozilla/geckoview/geckoview-nightly-x86/63.0.20180830111743/geckoview-nightly-x86-63.0.20180830111743.pom': '63.0.20180830111743',
        'maven2/org/mozilla/geckoview/geckoview-nightly-x86/64.0.20181018103737/geckoview-nightly-x86-64.0.20181018103737.pom': '64.0.20181018103737',
        'maven2/org/mozilla/geckoview/geckoview-nightly-x86/65.0.20181028102554/geckoview-nightly-x86-65.0.20181028102554.pom': '65.0.20181028102554',
        'maven2/org/mozilla/geckoview/geckoview-nightly-x86/63.0.20180830100125/geckoview-nightly-x86-63.0.20180830100125.pom': '63.0.20180830100125',
        'maven2/org/mozilla/geckoview/geckoview-nightly-x86/65.0.20181029100346/geckoview-nightly-x86-65.0.20181029100346.pom': '65.0.20181029100346',
        'maven2/org/mozilla/geckoview/geckoview-nightly-x86/64.0.20181019100100/geckoview-nightly-x86-64.0.20181019100100.pom': '64.0.20181019100100',
    }


@freeze_time('2018-10-29 16:00:30')
def test_generate_last_updated():
    assert generate_last_updated() == '20181029160030'


@pytest.mark.parametrize('versions_per_path, expected', ((
    {
        'maven2/org/mozilla/geckoview/geckoview-nightly-x86/64.0.20181018103737/geckoview-nightly-x86-64.0.20181018103737.pom': '64.0.20181018103737',
        'maven2/org/mozilla/geckoview/geckoview-nightly-x86/63.0.20180830111743/geckoview-nightly-x86-63.0.20180830111743.pom': '63.0.20180830111743',
        'maven2/org/mozilla/geckoview/geckoview-nightly-x86/65.0.20181028102554/geckoview-nightly-x86-65.0.20181028102554.pom': '65.0.20181028102554',
        'maven2/org/mozilla/geckoview/geckoview-nightly-x86/63.0.20180830100125/geckoview-nightly-x86-63.0.20180830100125.pom': '63.0.20180830100125',
        'maven2/org/mozilla/geckoview/geckoview-nightly-x86/65.0.20181029100346/geckoview-nightly-x86-65.0.20181029100346.pom': '65.0.20181029100346',
        'maven2/org/mozilla/geckoview/geckoview-nightly-x86/64.0.20181019100100/geckoview-nightly-x86-64.0.20181019100100.pom': '64.0.20181019100100',
    },
    '65.0.20181029100346',
), (
    {
        'some/path': '64.0',
    },
    '64.0',
), (
    {
        'some/path': '64.0',
    },
    '64.0',
), (
    {
        'some/path':'64.0',
        'some/other/path':'65.0',
    },
    '65.0',
)))
def test_get_latest_version(versions_per_path, expected):
    assert get_latest_version(versions_per_path) == expected


def test_get_latest_version_error():
    with pytest.raises(ValueError) as excinfo:
        versions_per_path = {
            'maven2/org/mozilla/geckoview/geckoview-nightly-x86/64.0-TESTING/geckoview-nightly-x86-64.0-TESTING.pom': '64.0-TESTING'
        }
        get_latest_version(versions_per_path)

    assert '"maven2/org/mozilla/geckoview/geckoview-nightly-x86/64.0-TESTING/geckoview-nightly-x86-64.0-TESTING.pom" does not contain a valid version. See root error.' in str(excinfo.value)


def test_upload_s3_file(monkeypatch):
    s3_mock = MagicMock()
    object_mock = MagicMock()
    s3_mock.Object.return_value = object_mock
    invalidate_cloudfront_mock = MagicMock()
    monkeypatch.setattr('maven_lambda.metadata.s3', s3_mock)
    monkeypatch.setattr('maven_lambda.metadata.invalidate_cloudfront_cache', invalidate_cloudfront_mock)
    assert upload_s3_file(
        'some_bucket', 'some/folder/', 'some_file', 'some data', content_type='some/content-type'
    ) == 'some/folder/some_file'

    s3_mock.Object.assert_called_once_with('some_bucket', 'some/folder/some_file')
    object_mock.put.assert_called_once_with(Body='some data', ContentType='some/content-type')


@pytest.mark.parametrize('cloudfront_distribution_id, paths, expected_items, expected_quantity', ((
    None, ['some/folder/some_file'], None, None
), (
    'some-id', ['some/folder/some_file'], ['/some/folder/some_file'], 1
), (
    'some-id', ['some/folder/some_file', '/another/folder/another_file'], ['/some/folder/some_file', '/another/folder/another_file'], 2
)))
def test_invalidate_cloudfront(monkeypatch, cloudfront_distribution_id, paths, expected_items, expected_quantity):
    cloudfront_mock = MagicMock()
    monkeypatch.setattr('maven_lambda.metadata.cloudfront', cloudfront_mock)
    monkeypatch.setattr('os.environ.get', lambda _, __: cloudfront_distribution_id)
    monkeypatch.setattr('slugid.nice', lambda: 'some_-Known_-_Slug--Id')

    invalidate_cloudfront_cache(paths)

    if cloudfront_distribution_id:
        cloudfront_mock.create_invalidation.assert_called_once_with(
            DistributionId='some-id',
            InvalidationBatch={
                'Paths': {
                    'Quantity': expected_quantity,
                    'Items': expected_items,
                },
                'CallerReference': 'some_-Known_-_Slug--Id',
            }
        )
    else:
        cloudfront_mock.create_invalidation.assert_not_called()


def test_invalidate_cloudfront_does_not_bail_out_on_client_error(monkeypatch):
    cloudfront_mock = MagicMock()
    monkeypatch.setattr('maven_lambda.metadata.cloudfront', cloudfront_mock)
    monkeypatch.setattr('os.environ.get', lambda _, __: 'some-id')
    monkeypatch.setattr('slugid.nice', lambda: 'some_-Known_-_Slug--Id')

    cloudfront_mock.create_invalidation.side_effect = ClientError({}, 'CreateInvalidation')

    invalidate_cloudfront_cache(['some/folder/some_file'])  # Does not raise


@pytest.mark.parametrize('data', (
    'known string',
    b'known string',
))
def test_generate_checksums(data):
    assert generate_checksums(data) == {
        'md5': 'a48fba03a9ac529b358935164826d9fe',
        'sha1': '714f4de20aa1899ed09e22a82304e12d4658eac1',
    }
