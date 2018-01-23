import sqlite3
from warnings import warn
from typing import Tuple

class NotCachedException(Exception):
    pass


class DictBackend(object):
    def __init__(self, num_bands, cache_documents=False):
        self._fingerprints = None
        self._doc_ids = [dict() for _ in range(num_bands)]
        self.num_bands = num_bands

    def is_empty(self):
        return self._doc_ids is None or not self._doc_ids

    def add_fingerprint(self, bin_i:int, bucket_id:int, fingerprint:Tuple[int], doc_id:int):
        """Add a fingerprint to the backend.

        The fingerprint is added to the backend storage, along with an optional document ID. If the document ID is
        specified and a previously stored document (not fingerprint) under that ID does not exist a warning is raised.

        Parameters
        ----------
        bin_i: int, the band index of the fingerprint
        bucket_id: int, the value of the fingerprint within the band
        fingerprint: int, the fingerprint to add
        doc_id: object, and optional document ID to store with the fingerprint
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

    def get_fingerprint(self, doc_id:int):
        if doc_id is None or doc_id not in self._fingerprints:
            raise KeyError(f'Tried to retrieve fingerprint for non existent document id:{doc_id}.')
        return self._fingerprints[doc_id]

    def get_document(self, doc_id):
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


class SqliteBackend(object):
    def __init__(self, num_bands, filename='lsh.sqlite'):
        self._conn = sqlite3.connect(filename)
        self._cursor = self.conn.cursor()
        self.filename = filename

        # check if DB is empty
        missing = []
        for band_i in range(num_bands):
            query = f'SELECT name FROM sqlite_master WHERE type"table" AND name="band-{band_i}";'
            _ = self.cursor.execute(query)

            if not self.cursor.fetchone():
                missing.append(band_i)

        for band_i in range(num_bands):
            query = f'CREATE TABLE "band-{band_i}" (bucket_id, fingerprint, doc_id);'
            _ = self.cursor.execute(query)

        self._conn.commit()
        self._conn.close()

    def add(self, bin_id, bucket_id, fingerprint, doc_id=None):
        self._conn = sqlite3.connect(self.filename)
        self._cursor = self.conn.cursor()

        query = f'INSERT INTO "band-{bin_id}" VALUES (bucket_id, fingerprint, doc_id);'
        _ = self.cursor.execute(query)

        self._conn.commit()
        self._conn.close()