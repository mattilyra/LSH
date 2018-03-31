import os
import sqlite3
import logging
import contextlib

from warnings import warn
from typing import Tuple, List, Dict, Set
from contextlib import contextmanager
from pathlib import Path


log = logging.getLogger(__name__)


def validate_docid(doc_id):
    if doc_id is None:
        raise ValueError('Document ID (doc_id) can not be None.')

    try:
        hash(doc_id)
    except TypeError:
        raise ValueError(f'Document ID must be a hashable type not {type(doc_id)}.')

    return doc_id


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

        doc_id = validate_docid(doc_id)

        if bucket_id not in self._doc_ids[bin_i]:
            self._doc_ids[bin_i][bucket_id] = set()
        self._doc_ids[bin_i][bucket_id].add(doc_id)

        if self._fingerprints is None:
            self._fingerprints = dict()
        self._fingerprints[doc_id] = fingerprint

    def get_all_buckets(self):
        yield from (bucket for bin_i in range(len(self._doc_ids)) for bucket in self._doc_ids[bin_i].values())

    def get_bucket(self, bin_i:int, bucket_id:int) -> Set[int]:
        """Returns the document IDs contained in a specific bucket."""
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
        self.empty = True

        if not self.filename.parent.exists():
            self.filename.parent.mkdir()

        if self.filename.exists():
            if self.num_bands != num_bands and num_bands > 0:
                raise RuntimeError(f'SqliteDB {filename} exists and contains {_num_bands_} bands, {num_bands} were requested. Please use -1 for existing files or delete the file.')
            else:
                with sqlite_cursor(self.filename) as c:
                    count = c.execute('SELECT COUNT(DISTINCT doc_id) from data').fetchone()
                    self.empty = count == 0
        else:
            self.initdb()

    def initdb(self):
        with sqlite_cursor(self.filename) as c:
            _ = c.execute(f'CREATE TABLE "data" (band_id, bucket_id, doc_id, fingerprint, f_ord);')
            _ = c.execute(f'CREATE INDEX bands on "data" (band_id, bucket_id);')
            _ = c.execute(f'CREATE INDEX docs on "data" (doc_id);')

    def is_empty(self):
        return self.empty

    def add_fingerprint(self, bins_buckets: List[Tuple[int, int]], fingerprint: Tuple[int], doc_id: int):
        doc_id = validate_docid(doc_id)
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
            if self.empty and rows > 0:
                self.emtpy = False
        return rows > 0

    def doc_exists(self, doc_id):
        if self.empty:
            return False

        with sqlite_cursor(self.filename) as c:
            c = c.execute(f'SELECT COUNT(doc_id) from data where doc_id={doc_id};')
            count = c.fetchone()[0]
        return count > 0

    def get_fingerprint(self, doc_id:int) -> Tuple[int, ...]:
        if self.empty:
            raise RuntimeError('Tried to retrieve fingerprint from an empty DB.')

        if doc_id is None or not self.doc_exists():
            raise KeyError(f'Tried to retrieve fingerprint for non existent document id: {doc_id}.')

        with sqlite_cursor(self.filename) as c:
            log.debug(f'SELECT band_id, bucket_id from data where doc_id={doc_id};')
            c = c.execute(f'SELECT DISTINCT band_id, bucket_id from data where doc_id={doc_id};')
            band, bucket = c.fetchone()
            c = c.execute(f'SELECT fingerprint from data WHERE band_id={band} AND bucket_id={bucket} ORDER BY f_ord;')
            fingerprint = c.fetchall()
            fingerprint = tuple(e['fingerprint'] for e in fingerprint)
        return fingerprint

    def get_bucket(self, bin_i:int, bucket_id:int) -> Set[int]:
        with sqlite_cursor(self.filename) as c:
            c = c.execute(f'SELECT DISTINCT doc_id FROM data where band_id={bin_i} and bucket_id={bucket_id}')
            if c.rowcount == 0:
                return {}

            doc_ids = set([r[0] for r in c.fetchall()])
        return doc_ids

    def get_all_buckets(self) -> List[Tuple[int, ...]]:
        with sqlite_cursor(self.filename) as c:
            c.execute('SELECT doc_id from data GROUP BY band_id, bucket_id')
            return c.fetchall()

    def remove_docid(self, bin_i, bucket_id, doc_id):
        with sqlite_cursor(self.filename) as c:
            c.execute(f'DELETE * FROM data WHERE doc_id={doc_id}')
            return c.rowcount > 0

    def remove_fingerprint(self, doc_id):
        return self.remove_docid(None, None, doc_id)

    def clear(self):
        self.empty = True
        with sqlite_cursor(self.filename) as c:
            c.execute('DELETE * from data;')