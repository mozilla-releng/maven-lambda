import pytest

from freezegun import freeze_time
from unittest.mock import MagicMock

from maven_lambda import (
    generate_checksums,
    generate_last_updated,
    generate_release_maven_metadata,
    generate_versions,
    get_artifact_id,
    get_folder_key,
    get_group_id,
    get_latest_version,
    get_version,
    lambda_handler,
    list_pom_files_in_subfolders,
    upload_s3_file,
)


def test_lambda_handler(monkeypatch):
    event = {
        'Records': [{
            's3': {
                'bucket': {
                    'name': 'some_bucket_name',
                },
                'object': {
                    'key': 'maven2/org/mozilla/geckoview/geckoview-nightly-x86/63.0.20180830111743/geckoview-nightly-x86-63.0.20180830111743.pom',     # noqa: E501
                },
            },
        }],
    }
    context = {}
    s3_mock = MagicMock()
    monkeypatch.setattr('maven_lambda.s3', s3_mock)
    monkeypatch.setattr('maven_lambda.list_pom_files_in_subfolders', lambda _, __: [
        'maven2/org/mozilla/geckoview/geckoview-nightly-x86/63.0.20180830111743/geckoview-nightly-x86-63.0.20180830111743.pom',         # noqa: E501
    ])
    monkeypatch.setattr('maven_lambda.upload_s3_file', lambda _, __, ___, ____, content_type=None: None)     # noqa: E501

    lambda_handler(event, context)
    s3_mock.Bucket.assert_called_once_with('some_bucket_name')

    def fail(_, __):
        raise ConnectionError()
    monkeypatch.setattr('maven_lambda.list_pom_files_in_subfolders', fail)
    with pytest.raises(ConnectionError):
        lambda_handler(event, context)


def test_get_folder_key():
    assert get_folder_key(
        'org/mozilla/geckoview/geckoview-nightly-x86/63.0.20180830111743/geckoview-nightly-x86-63.0.20180830111743.pom',     # noqa: E501
    ) == 'org/mozilla/geckoview/geckoview-nightly-x86/'


def test_list_pom_files_in_subfolders():
    bucket_mock = MagicMock()
    keys = [
        'maven2/org/mozilla/geckoview/geckoview-nightly-x86/63.0.20180830111743/geckoview-nightly-x86-63.0.20180830111743.pom',         # noqa: E501
        'maven2/org/mozilla/geckoview/geckoview-nightly-x86/63.0.20180830111743/geckoview-nightly-x86-63.0.20180830111743.pom.md5',     # noqa: E501
        'maven2/org/mozilla/geckoview/geckoview-nightly-x86/63.0.20180830111743/geckoview-nightly-x86-63.0.20180830111743.pom.sha1',    # noqa: E501
        'maven2/org/mozilla/geckoview/geckoview-nightly-x86/63.0.20180830111743/geckoview-nightly-x86-63.0.20180830111743.jar',         # noqa: E501
        'maven2/org/mozilla/geckoview/geckoview-nightly-x86/63.0.20180830111743/geckoview-nightly-x86-63.0.20180830111743.jar.md5',     # noqa: E501
        'maven2/org/mozilla/geckoview/geckoview-nightly-x86/63.0.20180830111743/geckoview-nightly-x86-63.0.20180830111743.jar.sha1',    # noqa: E501
        'maven2/org/mozilla/geckoview/geckoview-nightly-x86/64.0.20181018103737/geckoview-nightly-x86-64.0.20181018103737.pom',         # noqa: E501
        'maven2/org/mozilla/geckoview/geckoview-nightly-x86/64.0.20181018103737/geckoview-nightly-x86-64.0.20181018103737.jar',         # noqa: E501
    ]
    bucket_mock.objects.filter.return_value = [MagicMock(key=key) for key in keys]

    assert list_pom_files_in_subfolders(
        bucket_mock, 'maven2/org/mozilla/geckoview/geckoview-nightly-x86'
    ) == [
        'maven2/org/mozilla/geckoview/geckoview-nightly-x86/63.0.20180830111743/geckoview-nightly-x86-63.0.20180830111743.pom',     # noqa: E501
        'maven2/org/mozilla/geckoview/geckoview-nightly-x86/64.0.20181018103737/geckoview-nightly-x86-64.0.20181018103737.pom',     # noqa: E501
    ]
    bucket_mock.objects.filter.assert_called_once_with(
        Prefix='maven2/org/mozilla/geckoview/geckoview-nightly-x86'
    )


@pytest.mark.parametrize('key', (
    'maven2/org/mozilla/geckoview/geckoview-nightly-x86/63.0.20180830111743/geckoview-nightly-x86-63.0.20180830111743.pom',     # noqa: E501
    'org/mozilla/geckoview/geckoview-nightly-x86/63.0.20180830111743/geckoview-nightly-x86-63.0.20180830111743.pom',     # noqa: E501
))
def test_get_group_id(key):
    assert get_group_id(key) == 'org.mozilla.geckoview'


def test_get_artifact_id():
    assert get_artifact_id(
        'maven2/org/mozilla/geckoview/geckoview-nightly-x86/63.0.20180830111743/geckoview-nightly-x86-63.0.20180830111743.pom',     # noqa: E501
    ) == 'geckoview-nightly-x86'


def test_get_version():
    assert get_version(
        'maven2/org/mozilla/geckoview/geckoview-nightly-x86/63.0.20180830111743/geckoview-nightly-x86-63.0.20180830111743.pom',     # noqa: E501
    ) == '63.0.20180830111743'


@freeze_time('2018-10-29 16:00:30')
def test_generate_release_maven_metadata():
    assert generate_release_maven_metadata([
        'maven2/org/mozilla/geckoview/geckoview-nightly-x86/63.0.20180830111743/geckoview-nightly-x86-63.0.20180830111743.pom',     # noqa: E501
        'maven2/org/mozilla/geckoview/geckoview-nightly-x86/64.0.20181018103737/geckoview-nightly-x86-64.0.20181018103737.pom',     # noqa: E501
        'maven2/org/mozilla/geckoview/geckoview-nightly-x86/63.0.20180830111743/geckoview-nightly-x86-63.0.20180830111743.pom',     # noqa: E501
        'maven2/org/mozilla/geckoview/geckoview-nightly-x86/65.0.20181028102554/geckoview-nightly-x86-65.0.20181028102554.pom',     # noqa: E501
        'maven2/org/mozilla/geckoview/geckoview-nightly-x86/63.0.20180830100125/geckoview-nightly-x86-63.0.20180830100125.pom',     # noqa: E501
        'maven2/org/mozilla/geckoview/geckoview-nightly-x86/65.0.20181029100346/geckoview-nightly-x86-65.0.20181029100346.pom',     # noqa: E501
        'maven2/org/mozilla/geckoview/geckoview-nightly-x86/64.0.20181019100100/geckoview-nightly-x86-64.0.20181019100100.pom',     # noqa: E501
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


def test_generate_versions():
    assert generate_versions([
        'maven2/org/mozilla/geckoview/geckoview-nightly-x86/63.0.20180830111743/geckoview-nightly-x86-63.0.20180830111743.pom',     # noqa: E501
        'maven2/org/mozilla/geckoview/geckoview-nightly-x86/64.0.20181018103737/geckoview-nightly-x86-64.0.20181018103737.pom',     # noqa: E501
        'maven2/org/mozilla/geckoview/geckoview-nightly-x86/63.0.20180830111743/geckoview-nightly-x86-63.0.20180830111743.pom',     # noqa: E501
        'maven2/org/mozilla/geckoview/geckoview-nightly-x86/65.0.20181028102554/geckoview-nightly-x86-65.0.20181028102554.pom',     # noqa: E501
        'maven2/org/mozilla/geckoview/geckoview-nightly-x86/63.0.20180830100125/geckoview-nightly-x86-63.0.20180830100125.pom',     # noqa: E501
        'maven2/org/mozilla/geckoview/geckoview-nightly-x86/65.0.20181029100346/geckoview-nightly-x86-65.0.20181029100346.pom',     # noqa: E501
        'maven2/org/mozilla/geckoview/geckoview-nightly-x86/64.0.20181019100100/geckoview-nightly-x86-64.0.20181019100100.pom',     # noqa: E501
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


def test_get_latest_version():
    assert get_latest_version([
        '64.0.20181018103737',
        '63.0.20180830111743',
        '65.0.20181028102554',
        '63.0.20180830100125',
        '65.0.20181029100346',
        '64.0.20181019100100',
    ]) == '65.0.20181029100346'


def test_upload_s3_file(monkeypatch):
    s3_mock = MagicMock()
    object_mock = MagicMock()
    s3_mock.Object.return_value = object_mock
    monkeypatch.setattr('maven_lambda.s3', s3_mock)
    upload_s3_file(
        'some_bucket', 'some/folder/', 'some_file', 'some data', content_type='some/content-type'
    )

    s3_mock.Object.assert_called_once_with('some_bucket', 'some/folder/some_file')
    object_mock.put.assert_called_once_with(Body='some data', ContentType='some/content-type')


@pytest.mark.parametrize('data', (
    'known string',
    b'known string',
))
def test_generate_checksums(data):
    assert generate_checksums(data) == {
        'md5': 'a48fba03a9ac529b358935164826d9fe',
        'sha1': '714f4de20aa1899ed09e22a82304e12d4658eac1',
    }
