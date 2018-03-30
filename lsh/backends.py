import sqlite3
import logging

from warnings import warn
from typing import Tuple, List
from contextlib import contextmanager
from pathlib import Path


log = logging.getLogger(__name__)


class NotCachedException(Exception):
    pass


class DictBackend():
    def __init__(self, num_bands, cache_documents=False):
        self._fingerprints = None
        self._doc_ids = [dict() for _ in range(num_bands)]
        self.num_bands = num_bands

    def is_empty(self):
        return self._doc_ids is None or not self._doc_ids

    def add_fingerprint(self, bin_i:int, bucket_id:int, fingerprint:Tuple[int, ...], doc_id):
        """Add a fingerprint to the backend.

        The fingerprint is added to the backend storage, along with an optional document ID. If the document ID is
        specified and a previously stored document (not fingerprint) under that ID does not exist a warning is raised.

        Parameters
        ----------
        bin_i: int, the band index of the fingerprint
        bucket_id: int, the value of the fingerprint within the band
        fingerprint: int, the fingerprint to add
        doc_id: object, an optional document ID to store with the fingerprint
        """

        if doc_id is None:
            raise ValueError('Document ID (doc_id) can not be None.')

        try:
            hash(doc_id)
        except TypeError:
            raise ValueError(f'Document ID must be a hashable type not {type(doc_id)}.')

        if bucket_id not in self._doc_ids[bin_i]:
            self._doc_ids[bin_i][bucket_id] = set()
        self._doc_ids[bin_i][bucket_id].add(doc_id)

        if self._fingerprints is None:
            self._fingerprints = dict()
        self._fingerprints[doc_id] = fingerprint

    def get_all_buckets(self):
        yield from (bucket for bin_i in range(len(self._doc_ids)) for bucket in self._doc_ids[bin_i].values())

    def get_bucket(self, bin_i, bucket_id):
        if bucket_id not in self._doc_ids[bin_i]:
            return {}

        return self._doc_ids[bin_i][bucket_id]

    def get_fingerprint(self, doc_id:int) -> Tuple[int, ...]:
        if doc_id is None or doc_id not in self._fingerprints:
            raise KeyError(f'Tried to retrieve fingerprint for non existent document id: {doc_id}.')
        return self._fingerprints[doc_id]

    def get_document(self, doc_id:int) -> str:
        if self.cache_documents:
            return self._documents[doc_id]
        else:
            raise NotCachedException('Can not retrieve document by ID when cache_documents=False.')

    def remove_docid(self, bin_i, bucket_id, doc_id):
        self._doc_ids[bin_i][bucket_id].remove(doc_id)
        if not self._doc_ids[bin_i][bucket_id]:
            del self._doc_ids[bin_i][bucket_id]

    def remove_fingerprint(self, doc_id):
        del self._fingerprints[doc_id]

    def clear(self):
        self._doc_ids = [dict() for _ in range(self.num_bands)]
        self._fingerprints = None


@contextmanager
def sqlite_cursor(filename:Path) -> sqlite3.Cursor:
    try:
        filename = str(filename.expanduser().absolute())
    except AttributeError:
        pass
    conn = sqlite3.connect(filename)
    yield conn.cursor()
    conn.commit()
    conn.close()


class SqliteBackend():
    def __init__(self, num_bands, filename='lsh.sqlite'):
        self.filename = Path(filename).expanduser()
        self.num_bands = num_bands

        if not self.filename.parent.exists():
            self.filename.parent.mkdir()

        if self.filename.exists():
            if self.num_bands != num_bands and num_bands > 0:
                raise RuntimeError(f'SqliteDB {filename} exists and contains {_num_bands_} bands, {num_bands} were requested. Please use -1 for existing files or delete the file.')
        else:
            with sqlite_cursor(self.filename) as c:
                _ = c.execute(f'CREATE TABLE "data" (band_id, bucket_id, doc_id, fingerprint, f_ord);')
                _ = c.execute(f'CREATE INDEX bands on "data" (band_id, bucket_id);')
                _ = c.execute(f'CREATE INDEX docs on "data" (doc_id);')

    def add_fingerprint(self, bins_buckets: List[Tuple[int, int]], fingerprint: Tuple[int], doc_id: int):
        if self.doc_exists(doc_id):
            log.info(f'Document {doc_id} exists.')
            return False

        with sqlite_cursor(self.filename) as c:
            values = [(bin_i, bucket_id, doc_id, int(part), i)
                      for bin_i, bucket_id in bins_buckets
                      for i, part in enumerate(fingerprint)]
            query = f'INSERT INTO data VALUES (?, ?, ?, ?, ?);'
            log.debug(f'{query}')
            _ = c.executemany(query, values)
            rows = c.rowcount
        return rows > 0

    def doc_exists(self, doc_id):
        with sqlite_cursor(self.filename) as c:
            c = c.execute(f'SELECT COUNT(doc_id) from data where doc_id={doc_id};')
            count = c.fetchone()[0]
        return count > 0

    def get_fingerprint(self, doc_id:int) -> Tuple[int, ...]:
        if doc_id is None or not self.doc_exists():
            raise KeyError(f'Tried to retrieve fingerprint for non existent document id: {doc_id}.')

        with sqlite_cursor(self.filename) as c:
            log.debug(f'SELECT band_id, bucket_id from data where doc_id={doc_id};')
            c = c.execute(f'SELECT DISTINCT band_id, bucket_id from data where doc_id={doc_id};')
            band, bucket = c.fetchone()
            c = c.execute(f'SELECT fingerprint from data WHERE band_id={band} AND bucket_id={bucket} ORDER BY f_ord;')
            fingerprint = c.fetchall()
            fingerprint = tuple(e[0] for e in fingerprint)
        return fingerprint

    def get_bucket(self, bin_i:int, bucket_id:int) -> List[Tuple[int, ...]]:
        raise NotImplementedError()

    def get_all_buckets(self):
        raise NotImplementedError()
