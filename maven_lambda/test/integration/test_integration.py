from freezegun import freeze_time
from unittest.mock import MagicMock

from maven_lambda import lambda_handler


BUCKET_KEYS = [
    # Full content of a folder
    'maven2/org/mozilla/geckoview/geckoview-nightly-x86/63.0.20180830111743/geckoview-nightly-x86-63.0.20180830111743.pom',         # noqa: E501
    'maven2/org/mozilla/geckoview/geckoview-nightly-x86/63.0.20180830111743/geckoview-nightly-x86-63.0.20180830111743.pom.md5',     # noqa: E501
    'maven2/org/mozilla/geckoview/geckoview-nightly-x86/63.0.20180830111743/geckoview-nightly-x86-63.0.20180830111743.pom.sha1',    # noqa: E501
    'maven2/org/mozilla/geckoview/geckoview-nightly-x86/63.0.20180830111743/geckoview-nightly-x86-63.0.20180830111743.jar',         # noqa: E501
    'maven2/org/mozilla/geckoview/geckoview-nightly-x86/63.0.20180830111743/geckoview-nightly-x86-63.0.20180830111743.jar.md5',     # noqa: E501
    'maven2/org/mozilla/geckoview/geckoview-nightly-x86/63.0.20180830111743/geckoview-nightly-x86-63.0.20180830111743.jar.sha1',    # noqa: E501
    'maven2/org/mozilla/geckoview/geckoview-nightly-x86/63.0.20180830111743/geckoview-nightly-x86-63.0.20180830111743-sources.jar',         # noqa: E501
    'maven2/org/mozilla/geckoview/geckoview-nightly-x86/63.0.20180830111743/geckoview-nightly-x86-63.0.20180830111743-sources.jar.md5',     # noqa: E501
    'maven2/org/mozilla/geckoview/geckoview-nightly-x86/63.0.20180830111743/geckoview-nightly-x86-63.0.20180830111743-sources.jar.sha1',    # noqa: E501
    'maven2/org/mozilla/geckoview/geckoview-nightly-x86/63.0.20180830111743/geckoview-nightly-x86-63.0.20180830111743-javadoc.jar',         # noqa: E501
    'maven2/org/mozilla/geckoview/geckoview-nightly-x86/63.0.20180830111743/geckoview-nightly-x86-63.0.20180830111743-javadoc.jar.md5',     # noqa: E501
    'maven2/org/mozilla/geckoview/geckoview-nightly-x86/63.0.20180830111743/geckoview-nightly-x86-63.0.20180830111743-javadoc.jar.sha1',    # noqa: E501

    # Just partial content
    'maven2/org/mozilla/geckoview/geckoview-nightly-x86/63.0.20180830100125/geckoview-nightly-x86-63.0.20180830100125.pom',         # noqa: E501
    'maven2/org/mozilla/geckoview/geckoview-nightly-x86/63.0.20180830100125/geckoview-nightly-x86-63.0.20180830100125.pom.md5',     # noqa: E501
    'maven2/org/mozilla/geckoview/geckoview-nightly-x86/63.0.20180830100125/geckoview-nightly-x86-63.0.20180830100125.pom.sha1',    # noqa: E501
    'maven2/org/mozilla/geckoview/geckoview-nightly-x86/63.0.20180830100125/geckoview-nightly-x86-63.0.20180830100125.jar',         # noqa: E501

    'maven2/org/mozilla/geckoview/geckoview-nightly-x86/63.0.20180830111743/geckoview-nightly-x86-63.0.20180830111743.pom',         # noqa: E501
    'maven2/org/mozilla/geckoview/geckoview-nightly-x86/63.0.20180830111743/geckoview-nightly-x86-63.0.20180830111743.pom.md5',     # noqa: E501
    'maven2/org/mozilla/geckoview/geckoview-nightly-x86/63.0.20180830111743/geckoview-nightly-x86-63.0.20180830111743.pom.sha1',    # noqa: E501
    'maven2/org/mozilla/geckoview/geckoview-nightly-x86/63.0.20180830111743/geckoview-nightly-x86-63.0.20180830111743.jar',         # noqa: E501

    'maven2/org/mozilla/geckoview/geckoview-nightly-x86/64.0.20181018103737/geckoview-nightly-x86-64.0.20181018103737.pom',         # noqa: E501
    'maven2/org/mozilla/geckoview/geckoview-nightly-x86/64.0.20181018103737/geckoview-nightly-x86-64.0.20181018103737.pom.md5',     # noqa: E501
    'maven2/org/mozilla/geckoview/geckoview-nightly-x86/64.0.20181018103737/geckoview-nightly-x86-64.0.20181018103737.pom.sha1',    # noqa: E501
    'maven2/org/mozilla/geckoview/geckoview-nightly-x86/64.0.20181018103737/geckoview-nightly-x86-64.0.20181018103737.jar',         # noqa: E501

    'maven2/org/mozilla/geckoview/geckoview-nightly-x86/64.0.20181019100100/geckoview-nightly-x86-64.0.20181019100100.pom',         # noqa: E501
    'maven2/org/mozilla/geckoview/geckoview-nightly-x86/64.0.20181019100100/geckoview-nightly-x86-64.0.20181019100100.pom.md5',     # noqa: E501
    'maven2/org/mozilla/geckoview/geckoview-nightly-x86/64.0.20181019100100/geckoview-nightly-x86-64.0.20181019100100.pom.sha1',    # noqa: E501
    'maven2/org/mozilla/geckoview/geckoview-nightly-x86/64.0.20181019100100/geckoview-nightly-x86-64.0.20181019100100.jar',         # noqa: E501

    'maven2/org/mozilla/geckoview/geckoview-nightly-x86/65.0.20181028102554/geckoview-nightly-x86-65.0.20181028102554.pom',         # noqa: E501
    'maven2/org/mozilla/geckoview/geckoview-nightly-x86/65.0.20181028102554/geckoview-nightly-x86-65.0.20181028102554.pom.md5',     # noqa: E501
    'maven2/org/mozilla/geckoview/geckoview-nightly-x86/65.0.20181028102554/geckoview-nightly-x86-65.0.20181028102554.pom.sha1',    # noqa: E501
    'maven2/org/mozilla/geckoview/geckoview-nightly-x86/65.0.20181028102554/geckoview-nightly-x86-65.0.20181028102554.jar',         # noqa: E501

    'maven2/org/mozilla/geckoview/geckoview-nightly-x86/65.0.20181029100346/geckoview-nightly-x86-65.0.20181029100346.pom',         # noqa: E501
    'maven2/org/mozilla/geckoview/geckoview-nightly-x86/65.0.20181029100346/geckoview-nightly-x86-65.0.20181029100346.pom.md5',     # noqa: E501
    'maven2/org/mozilla/geckoview/geckoview-nightly-x86/65.0.20181029100346/geckoview-nightly-x86-65.0.20181029100346.pom.sha1',    # noqa: E501
    'maven2/org/mozilla/geckoview/geckoview-nightly-x86/65.0.20181029100346/geckoview-nightly-x86-65.0.20181029100346.jar',         # noqa: E501
]


EXPECTED_XML = (
    "<?xml version='1.0' encoding='UTF-8'?>\n"
    "<metadata>"
        "<groupId>org.mozilla.geckoview</groupId>"          # noqa: E131
        "<artifactId>geckoview-nightly-x86</artifactId>"
        "<versioning>"
            "<latest>65.0.20181029100346</latest>"          # noqa: E131
            "<release>65.0.20181029100346</release>"
            "<versions>"
                "<version>63.0.20180830100125</version>"    # noqa: E131
                "<version>63.0.20180830111743</version>"
                "<version>64.0.20181018103737</version>"
                "<version>64.0.20181019100100</version>"
                "<version>65.0.20181028102554</version>"
                "<version>65.0.20181029100346</version>"
            "</versions>"
            "<lastUpdated>20181029160030</lastUpdated>"
        "</versioning>"
    "</metadata>"
)


@freeze_time('2018-10-29 16:00:30')
def test_lambda_handler(monkeypatch):
    event = {
        'Records': [{
            's3': {
                'bucket': {
                    'name': 'some_bucket_name',
                },
                'object': {
                    'key': 'maven2/org/mozilla/geckoview/geckoview-nightly-x86/65.0.20181029100346/geckoview-nightly-x86-65.0.20181029100346.pom',     # noqa: E501
                },
            },
        }],
    }
    context = {}

    s3_mock = MagicMock()
    bucket_mock = MagicMock()
    object_mock = MagicMock()

    bucket_mock.objects.filter.return_value = [MagicMock(key=key) for key in BUCKET_KEYS]
    s3_mock.Bucket.return_value = bucket_mock
    s3_mock.Object.return_value = object_mock

    monkeypatch.setattr('maven_lambda.s3', s3_mock)

    lambda_handler(event, context)

    s3_mock.Bucket.assert_called_once_with('some_bucket_name')
    bucket_mock.objects.filter.assert_called_once_with(
        Prefix='maven2/org/mozilla/geckoview/geckoview-nightly-x86/'
    )

    assert s3_mock.Object.call_count == 3
    assert object_mock.put.call_count == 3

    s3_mock.Object.assert_any_call(
        'some_bucket_name', 'maven2/org/mozilla/geckoview/geckoview-nightly-x86/maven-metadata.xml'
    )
    object_mock.put.assert_any_call(Body=EXPECTED_XML, ContentType='text/xml')
    s3_mock.Object.assert_any_call(
        'some_bucket_name',
        'maven2/org/mozilla/geckoview/geckoview-nightly-x86/maven-metadata.xml.md5'
    )
    object_mock.put.assert_any_call(
        Body='dbb16613f87336f53724b13ffe29f2b9', ContentType='text/plain'
    )
    s3_mock.Object.assert_any_call(
        'some_bucket_name',
        'maven2/org/mozilla/geckoview/geckoview-nightly-x86/maven-metadata.xml.sha1'
    )
    object_mock.put.assert_any_call(
        Body='2e2e61779fe78ce649ddfca8838e06dc54b0e40e', ContentType='text/plain'
    )
