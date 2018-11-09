import pytest

from freezegun import freeze_time
from unittest.mock import MagicMock, call

from maven_lambda import lambda_handler


@freeze_time('2018-10-29 16:00:30')
@pytest.mark.parametrize('inserted_key, bucket_keys, expected_prefix, expected_metadata', ((
    'maven2/org/mozilla/geckoview/geckoview-nightly-x86/65.0.20181029100346/geckoview-nightly-x86-65.0.20181029100346.pom',             # noqa: E501
    (
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
    ),
    'maven2/org/mozilla/geckoview/geckoview-nightly-x86/',
    {
        'version_metadata': {
            'md5_data': 'dbb16613f87336f53724b13ffe29f2b9',
            'md5_key': 'maven2/org/mozilla/geckoview/geckoview-nightly-x86/maven-metadata.xml.md5',
            'sha1_data': '2e2e61779fe78ce649ddfca8838e06dc54b0e40e',
            'sha1_key': 'maven2/org/mozilla/geckoview/geckoview-nightly-x86/maven-metadata.xml.sha1',
            'xml_data': (
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
            ),
            'xml_key': 'maven2/org/mozilla/geckoview/geckoview-nightly-x86/maven-metadata.xml',
        },
    },
), (
    'maven2/org/mozilla/components/browser-domains/0.30.0-SNAPSHOT/browser-domains-0.30.0-20181029.154529-1.pom',                   # noqa: E501
    (
        # Full content of a folder
        'maven2/org/mozilla/components/browser-domains/0.30.0-SNAPSHOT/browser-domains-0.30.0-20181029.154529-1.pom',               # noqa: E501
        'maven2/org/mozilla/components/browser-domains/0.30.0-SNAPSHOT/browser-domains-0.30.0-20181029.154529-1.pom.md5',           # noqa: E501
        'maven2/org/mozilla/components/browser-domains/0.30.0-SNAPSHOT/browser-domains-0.30.0-20181029.154529-1.pom.sha1',          # noqa: E501
        'maven2/org/mozilla/components/browser-domains/0.30.0-SNAPSHOT/browser-domains-0.30.0-20181029.154529-1.jar',               # noqa: E501
        'maven2/org/mozilla/components/browser-domains/0.30.0-SNAPSHOT/browser-domains-0.30.0-20181029.154529-1.jar.md5',           # noqa: E501
        'maven2/org/mozilla/components/browser-domains/0.30.0-SNAPSHOT/browser-domains-0.30.0-20181029.154529-1.jar.sha1',          # noqa: E501
        'maven2/org/mozilla/components/browser-domains/0.30.0-SNAPSHOT/browser-domains-0.30.0-20181029.154529-1-sources.jar',       # noqa: E501
        'maven2/org/mozilla/components/browser-domains/0.30.0-SNAPSHOT/browser-domains-0.30.0-20181029.154529-1-sources.jar.md5',   # noqa: E501
        'maven2/org/mozilla/components/browser-domains/0.30.0-SNAPSHOT/browser-domains-0.30.0-20181029.154529-1-sources.jar.sha1',  # noqa: E501
        'maven2/org/mozilla/components/browser-domains/0.30.0-SNAPSHOT/browser-domains-0.30.0-20181029.154529-1-javadoc.jar',       # noqa: E501
        'maven2/org/mozilla/components/browser-domains/0.30.0-SNAPSHOT/browser-domains-0.30.0-20181029.154529-1-javadoc.jar.md5',   # noqa: E501
        'maven2/org/mozilla/components/browser-domains/0.30.0-SNAPSHOT/browser-domains-0.30.0-20181029.154529-1-javadoc.jar.sha1',  # noqa: E501
    ),
    'maven2/org/mozilla/components/browser-domains/',
    {
        'snapshot_metadata': {
            'md5_data': '263d4ab1c3fed3aa462f7f64c5445856',
            'md5_key': 'maven2/org/mozilla/components/browser-domains/0.30.0-SNAPSHOT/maven-metadata.xml.md5',      # noqa: E501
            'prefix': 'maven2/org/mozilla/components/browser-domains/',
            'sha1_data': '286e6535dd95d8ab94f5ba6433183d40f88814e4',
            'sha1_key': 'maven2/org/mozilla/components/browser-domains/0.30.0-SNAPSHOT/maven-metadata.xml.sha1',    # noqa: E501
            'xml_data': (
                "<?xml version='1.0' encoding='UTF-8'?>\n"
                "<metadata>"
                  "<groupId>org.mozilla.components</groupId>"
                  "<artifactId>browser-domains</artifactId>"
                  "<version>0.30.0-SNAPSHOT</version>"
                  "<versioning>"
                    "<snapshot>"
                      "<timestamp>20181029.154529</timestamp>"
                      "<buildNumber>1</buildNumber>"
                    "</snapshot>"
                    "<lastUpdated>20181029160030</lastUpdated>"
                    "<snapshotVersions>"
                      "<snapshotVersion>"
                        "<extension>aar</extension>"
                        "<value>0.30.0-20181029.154529-1</value>"
                        "<updated>20181029154529</updated>"
                        "</snapshotVersion>"
                      "</snapshotVersions>"
                  "</versioning>"
                "</metadata>"
            ),
            'xml_key': 'maven2/org/mozilla/components/browser-domains/0.30.0-SNAPSHOT/maven-metadata.xml',          # noqa: E501
        },
        'version_metadata': {
            'md5_data': '3bf0e86cacbce020e5e041f026455160',
            'md5_key': 'maven2/org/mozilla/components/browser-domains/maven-metadata.xml.md5',

            'sha1_data': '10a78e44df3427953f0f3bd5919568a89a43ce90',
            'sha1_key': 'maven2/org/mozilla/components/browser-domains/maven-metadata.xml.sha1',
            'xml_data': (
                "<?xml version='1.0' encoding='UTF-8'?>\n"
                "<metadata>"
                    "<groupId>org.mozilla.components</groupId>"      # noqa: E131
                    "<artifactId>browser-domains</artifactId>"
                    "<versioning>"
                        "<latest>0.30.0-SNAPSHOT</latest>"          # noqa: E131
                        "<release></release>"
                        "<versions>"
                            "<version>0.30.0-SNAPSHOT</version>"    # noqa: E131
                        "</versions>"
                        "<lastUpdated>20181029160030</lastUpdated>"
                    "</versioning>"
                "</metadata>"
            ),
            'xml_key': 'maven2/org/mozilla/components/browser-domains/maven-metadata.xml',
        },
    },
)))
def test_lambda_handler(monkeypatch, inserted_key, bucket_keys, expected_prefix, expected_metadata):
    event = {
        'Records': [{
            's3': {
                'bucket': {
                    'name': 'some_bucket_name',
                },
                'object': {
                    'key': inserted_key,
                },
            },
        }],
    }
    context = {}

    s3_mock = MagicMock()
    bucket_mock = MagicMock()
    bucket_mock.name = 'some_bucket_name'   # "name" is an argument to the Mock constructor
    object_mock = MagicMock()

    bucket_mock.objects.filter.return_value = [MagicMock(key=key) for key in bucket_keys]
    s3_mock.Bucket.return_value = bucket_mock
    s3_mock.Object.return_value = object_mock

    monkeypatch.setattr('maven_lambda.s3', s3_mock)

    lambda_handler(event, context)

    s3_mock.Bucket.assert_called_once_with('some_bucket_name')
    bucket_mock.objects.filter.assert_called_once_with(Prefix=expected_prefix)

    for expected_item in expected_metadata.values():
        assert call('some_bucket_name', expected_item['xml_key']) in s3_mock.Object.call_args_list
        assert call(Body=expected_item['xml_data'], ContentType='text/xml') in object_mock.put.call_args_list
        assert call('some_bucket_name', expected_item['md5_key']) in s3_mock.Object.call_args_list
        assert call(Body=expected_item['md5_data'], ContentType='text/plain') in object_mock.put.call_args_list
        assert call('some_bucket_name', expected_item['sha1_key']) in s3_mock.Object.call_args_list
        assert call(Body=expected_item['sha1_data'], ContentType='text/plain') in object_mock.put.call_args_list

    expected_call_count = len(expected_metadata) * 3
    assert s3_mock.Object.call_count == expected_call_count
    assert object_mock.put.call_count == expected_call_count
