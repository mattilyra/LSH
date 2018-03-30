import os
from pathlib import Path
import tempfile

import pytest

from lsh.backends import DictBackend, SqliteBackend

@pytest.mark.parametrize("num_bands", [2, 4, 5, 8, 10, 25])
@pytest.fixture
def dict_db(num_bands):
    return DictBackend(num_bands)


def test_sqlite_mkdir(tmpdir, filename):
    p = Path(tmpdir, filename)
    _ = SqliteBackend(15, filename=filename)
    assert p.exists()


def test_sqlite_existing_db(filename):
    _ = SqliteBackend(15, filename=filename)
    path = Path(filename)
    assert path.exists()

    # should NOT throw an error
    _ = SqliteBackend(15, filename=filename)

    # should throw an error
    with pytest.raises(RuntimeError):
        _ = SqliteBackend(10, filename=filename)


# @pytest.mark.parametrize("cache_documents", [True, False])
# def test_init(dict_db, cache):
#     db = dict_db
#
#
# @pytest.mark.parametrize("cache_documents", [True, False])
# def test_add_fingerprint(dict_db, cache):
