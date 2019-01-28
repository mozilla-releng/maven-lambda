from unittest.mock import MagicMock
import pytest

from maven_lambda.copy import (
    s3_object_has_more_than_one_version,
    NotFound,
)

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
