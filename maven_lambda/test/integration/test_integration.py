import pytest

from freezegun import freeze_time
from unittest.mock import MagicMock, call

from maven_lambda.metadata import lambda_handler


@freeze_time('2018-10-29 16:00:30')
@pytest.mark.parametrize('inserted_key, bucket_keys, expected_prefix, expected_metadata', ((
    'maven2/org/mozilla/geckoview/geckoview-nightly-x86/65.0.20181029100346/geckoview-nightly-x86-65.0.20181029100346.pom',
    (
        # Full content of a folder
        'maven2/org/mozilla/geckoview/geckoview-nightly-x86/63.0.20180830111743/geckoview-nightly-x86-63.0.20180830111743.pom',
        'maven2/org/mozilla/geckoview/geckoview-nightly-x86/63.0.20180830111743/geckoview-nightly-x86-63.0.20180830111743.pom.md5',
        'maven2/org/mozilla/geckoview/geckoview-nightly-x86/63.0.20180830111743/geckoview-nightly-x86-63.0.20180830111743.pom.sha1',
        'maven2/org/mozilla/geckoview/geckoview-nightly-x86/63.0.20180830111743/geckoview-nightly-x86-63.0.20180830111743.jar',
        'maven2/org/mozilla/geckoview/geckoview-nightly-x86/63.0.20180830111743/geckoview-nightly-x86-63.0.20180830111743.jar.md5',
        'maven2/org/mozilla/geckoview/geckoview-nightly-x86/63.0.20180830111743/geckoview-nightly-x86-63.0.20180830111743.jar.sha1',
        'maven2/org/mozilla/geckoview/geckoview-nightly-x86/63.0.20180830111743/geckoview-nightly-x86-63.0.20180830111743-sources.jar',
        'maven2/org/mozilla/geckoview/geckoview-nightly-x86/63.0.20180830111743/geckoview-nightly-x86-63.0.20180830111743-sources.jar.md5',
        'maven2/org/mozilla/geckoview/geckoview-nightly-x86/63.0.20180830111743/geckoview-nightly-x86-63.0.20180830111743-sources.jar.sha1',
        'maven2/org/mozilla/geckoview/geckoview-nightly-x86/63.0.20180830111743/geckoview-nightly-x86-63.0.20180830111743-javadoc.jar',
        'maven2/org/mozilla/geckoview/geckoview-nightly-x86/63.0.20180830111743/geckoview-nightly-x86-63.0.20180830111743-javadoc.jar.md5',
        'maven2/org/mozilla/geckoview/geckoview-nightly-x86/63.0.20180830111743/geckoview-nightly-x86-63.0.20180830111743-javadoc.jar.sha1',

        # Just partial content
        'maven2/org/mozilla/geckoview/geckoview-nightly-x86/63.0.20180830100125/geckoview-nightly-x86-63.0.20180830100125.pom',
        'maven2/org/mozilla/geckoview/geckoview-nightly-x86/63.0.20180830100125/geckoview-nightly-x86-63.0.20180830100125.pom.md5',
        'maven2/org/mozilla/geckoview/geckoview-nightly-x86/63.0.20180830100125/geckoview-nightly-x86-63.0.20180830100125.pom.sha1',
        'maven2/org/mozilla/geckoview/geckoview-nightly-x86/63.0.20180830100125/geckoview-nightly-x86-63.0.20180830100125.jar',

        'maven2/org/mozilla/geckoview/geckoview-nightly-x86/63.0.20180830111743/geckoview-nightly-x86-63.0.20180830111743.pom',
        'maven2/org/mozilla/geckoview/geckoview-nightly-x86/63.0.20180830111743/geckoview-nightly-x86-63.0.20180830111743.pom.md5',
        'maven2/org/mozilla/geckoview/geckoview-nightly-x86/63.0.20180830111743/geckoview-nightly-x86-63.0.20180830111743.pom.sha1',
        'maven2/org/mozilla/geckoview/geckoview-nightly-x86/63.0.20180830111743/geckoview-nightly-x86-63.0.20180830111743.jar',

        'maven2/org/mozilla/geckoview/geckoview-nightly-x86/64.0.20181018103737/geckoview-nightly-x86-64.0.20181018103737.pom',
        'maven2/org/mozilla/geckoview/geckoview-nightly-x86/64.0.20181018103737/geckoview-nightly-x86-64.0.20181018103737.pom.md5',
        'maven2/org/mozilla/geckoview/geckoview-nightly-x86/64.0.20181018103737/geckoview-nightly-x86-64.0.20181018103737.pom.sha1',
        'maven2/org/mozilla/geckoview/geckoview-nightly-x86/64.0.20181018103737/geckoview-nightly-x86-64.0.20181018103737.jar',

        'maven2/org/mozilla/geckoview/geckoview-nightly-x86/64.0.20181019100100/geckoview-nightly-x86-64.0.20181019100100.pom',
        'maven2/org/mozilla/geckoview/geckoview-nightly-x86/64.0.20181019100100/geckoview-nightly-x86-64.0.20181019100100.pom.md5',
        'maven2/org/mozilla/geckoview/geckoview-nightly-x86/64.0.20181019100100/geckoview-nightly-x86-64.0.20181019100100.pom.sha1',
        'maven2/org/mozilla/geckoview/geckoview-nightly-x86/64.0.20181019100100/geckoview-nightly-x86-64.0.20181019100100.jar',

        'maven2/org/mozilla/geckoview/geckoview-nightly-x86/65.0.20181028102554/geckoview-nightly-x86-65.0.20181028102554.pom',
        'maven2/org/mozilla/geckoview/geckoview-nightly-x86/65.0.20181028102554/geckoview-nightly-x86-65.0.20181028102554.pom.md5',
        'maven2/org/mozilla/geckoview/geckoview-nightly-x86/65.0.20181028102554/geckoview-nightly-x86-65.0.20181028102554.pom.sha1',
        'maven2/org/mozilla/geckoview/geckoview-nightly-x86/65.0.20181028102554/geckoview-nightly-x86-65.0.20181028102554.jar',

        'maven2/org/mozilla/geckoview/geckoview-nightly-x86/65.0.20181029100346/geckoview-nightly-x86-65.0.20181029100346.pom',
        'maven2/org/mozilla/geckoview/geckoview-nightly-x86/65.0.20181029100346/geckoview-nightly-x86-65.0.20181029100346.pom.md5',
        'maven2/org/mozilla/geckoview/geckoview-nightly-x86/65.0.20181029100346/geckoview-nightly-x86-65.0.20181029100346.pom.sha1',
        'maven2/org/mozilla/geckoview/geckoview-nightly-x86/65.0.20181029100346/geckoview-nightly-x86-65.0.20181029100346.jar',
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
                "</metadata>"
            ),
            'xml_key': 'maven2/org/mozilla/geckoview/geckoview-nightly-x86/maven-metadata.xml',
        },
    },
), (
    'maven2/org/mozilla/components/browser-domains/0.30.0-SNAPSHOT/browser-domains-0.30.0-20181030.164630-2.pom',
    (
        # Full content of a folder
        'maven2/org/mozilla/components/browser-domains/0.30.0-SNAPSHOT/browser-domains-0.30.0-20181030.164630-2.pom',
        'maven2/org/mozilla/components/browser-domains/0.30.0-SNAPSHOT/browser-domains-0.30.0-20181030.164630-2.pom.md5',
        'maven2/org/mozilla/components/browser-domains/0.30.0-SNAPSHOT/browser-domains-0.30.0-20181030.164630-2.pom.sha1',
        'maven2/org/mozilla/components/browser-domains/0.30.0-SNAPSHOT/browser-domains-0.30.0-20181030.164630-2.jar',
        'maven2/org/mozilla/components/browser-domains/0.30.0-SNAPSHOT/browser-domains-0.30.0-20181030.164630-2.jar.md5',
        'maven2/org/mozilla/components/browser-domains/0.30.0-SNAPSHOT/browser-domains-0.30.0-20181030.164630-2.jar.sha1',
        'maven2/org/mozilla/components/browser-domains/0.30.0-SNAPSHOT/browser-domains-0.30.0-20181030.164630-2-sources.jar',
        'maven2/org/mozilla/components/browser-domains/0.30.0-SNAPSHOT/browser-domains-0.30.0-20181030.164630-2-sources.jar.md5',
        'maven2/org/mozilla/components/browser-domains/0.30.0-SNAPSHOT/browser-domains-0.30.0-20181030.164630-2-sources.jar.sha1',
        'maven2/org/mozilla/components/browser-domains/0.30.0-SNAPSHOT/browser-domains-0.30.0-20181030.164630-2-javadoc.jar',
        'maven2/org/mozilla/components/browser-domains/0.30.0-SNAPSHOT/browser-domains-0.30.0-20181030.164630-2-javadoc.jar.md5',
        'maven2/org/mozilla/components/browser-domains/0.30.0-SNAPSHOT/browser-domains-0.30.0-20181030.164630-2-javadoc.jar.sha1',

        # Partial content only
        'maven2/org/mozilla/components/browser-domains/0.30.0-SNAPSHOT/browser-domains-0.30.0-20181029.154529-1.pom',
    ),
    'maven2/org/mozilla/components/browser-domains/',
    {
        'snapshot_metadata': {
            'md5_data': '3e61e09c875afb80475d7acc857dc5c0',
            'md5_key': 'maven2/org/mozilla/components/browser-domains/0.30.0-SNAPSHOT/maven-metadata.xml.md5',
            'prefix': 'maven2/org/mozilla/components/browser-domains/',
            'sha1_data': '9b3cb2ca04652606e543957ca954c4f2a9a39d5b',
            'sha1_key': 'maven2/org/mozilla/components/browser-domains/0.30.0-SNAPSHOT/maven-metadata.xml.sha1',
            'xml_data': (
                "<?xml version='1.0' encoding='UTF-8'?>\n"
                "<metadata>"
                  "<groupId>org.mozilla.components</groupId>"
                  "<artifactId>browser-domains</artifactId>"
                  "<version>0.30.0-SNAPSHOT</version>"
                  "<versioning>"
                    "<snapshot>"
                      "<timestamp>20181030.164630</timestamp>"
                      "<buildNumber>2</buildNumber>"
                    "</snapshot>"
                    "<lastUpdated>20181029160030</lastUpdated>"
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
                "</metadata>"
            ),
            'xml_key': 'maven2/org/mozilla/components/browser-domains/0.30.0-SNAPSHOT/maven-metadata.xml',
        },
        'version_metadata': {
            'md5_data': '3bf0e86cacbce020e5e041f026455160',
            'md5_key': 'maven2/org/mozilla/components/browser-domains/maven-metadata.xml.md5',

            'sha1_data': '10a78e44df3427953f0f3bd5919568a89a43ce90',
            'sha1_key': 'maven2/org/mozilla/components/browser-domains/maven-metadata.xml.sha1',
            'xml_data': (
                "<?xml version='1.0' encoding='UTF-8'?>\n"
                "<metadata>"
                    "<groupId>org.mozilla.components</groupId>"
                    "<artifactId>browser-domains</artifactId>"
                    "<versioning>"
                        "<latest>0.30.0-SNAPSHOT</latest>"
                        "<release></release>"
                        "<versions>"
                            "<version>0.30.0-SNAPSHOT</version>"
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

    def fake_download(_, destination):
        with open(destination, 'w') as f:
            f.write('''<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0">
  <packaging>aar</packaging>
</project>''')

    bucket_mock.download_file.side_effect = fake_download
    bucket_mock.objects.filter.return_value = [MagicMock(key=key) for key in bucket_keys]
    s3_mock.Bucket.return_value = bucket_mock

    object_mock = MagicMock()
    s3_mock.Object.return_value = object_mock

    monkeypatch.setattr('maven_lambda.metadata.s3', s3_mock)

    lambda_handler(event, context)

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
