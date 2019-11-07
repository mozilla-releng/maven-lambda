from unittest.mock import MagicMock
import pytest

from maven_lambda.copy import (
    lambda_handler,
    s3_object_has_more_than_one_version,
    NotFound,
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
    monkeypatch.setattr('boto3.client', lambda _: s3_mock)
    monkeypatch.setattr('os.environ', {'TARGET_BUCKET': 'some_target_bucket'})

    monkeypatch.setattr('maven_lambda.copy.s3_object_has_more_than_one_version', lambda _, __, ___: False)
    assert lambda_handler(event, context) == {"statusCode": 200}
    s3_mock.copy_object.assert_called_once_with(
        Bucket='some_target_bucket',
        CopySource={
            'Bucket': 'some_bucket_name',
            'Key': 'maven2/org/mozilla/geckoview/geckoview-nightly-x86/63.0.20180830111743/geckoview-nightly-x86-63.0.20180830111743.pom',
        },
        Key='maven2/org/mozilla/geckoview/geckoview-nightly-x86/63.0.20180830111743/geckoview-nightly-x86-63.0.20180830111743.pom',
    )
    s3_mock.reset_mock()

    monkeypatch.setattr('maven_lambda.copy.s3_object_has_more_than_one_version', lambda _, __, ___: True)
    assert lambda_handler(event, context) == {"statusCode": 409}
    s3_mock.copy_object.assert_not_called()

    def fail(_, __, ___):
        raise NotFound()
    monkeypatch.setattr('maven_lambda.copy.s3_object_has_more_than_one_version', fail)
    assert lambda_handler(event, context) == {"statusCode": 404}
    s3_mock.copy_object.assert_not_called()


@pytest.mark.parametrize('versions, expected', ((
    {"Versions": [{"Key": "obj"}, {"Key": "obj"}]},
    True,
), (
    {"Versions": [{"Key": "obj"}, {"Key": "some_other_obj"}]},
    False,
)))
def test_s3_object_has_more_than_one_version_single_version(versions, expected):
    s3 = MagicMock()
    s3.list_object_versions.return_value = versions
    assert s3_object_has_more_than_one_version(s3, 'some_bucket', 'obj') == expected

def test_s3_object_has_more_than_one_version_single_version_not_found():
    s3 = MagicMock()
    s3.list_object_versions.return_value = {}
    with pytest.raises(NotFound):
        s3_object_has_more_than_one_version(s3, 'some_bucket', 'obj')
