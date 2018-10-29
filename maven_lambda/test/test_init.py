from maven_lambda import generate_checksums


def test_generate_checksums():
    assert generate_checksums(b'known string') == {
        'md5': 'a48fba03a9ac529b358935164826d9fe',
        'sha1': '714f4de20aa1899ed09e22a82304e12d4658eac1',
    }
