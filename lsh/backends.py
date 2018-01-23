import sqlite3
from warnings import warn


class NotCachedException(Exception):
    pass


class DictBackend(object):
    def __init__(self, num_bands, cache_documents=False):
        self._fingerprints = [dict() for _ in range(num_bands)]
        self._documents = {}
        self.cache_documents = cache_documents
        self._doc_ids = None

    def is_empty(self):
        return self._doc_ids is None or not self._doc_ids

    def add_fingerprint(self, bin_i, bucket_id, fingerprint, doc_id=None):
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
        if bucket_id not in self._fingerprints[bin_i]:
            self._fingerprints[bin_i][bucket_id] = set()
        self._fingerprints[bin_i][bucket_id].add(fingerprint)

        if doc_id is not None:
            # check that a document for DOCID exists
            # this is needed for later when exact similarities are computed
            # the fingerprints allow only approximate similarities to be
            # computed
            if self.cache_documents and doc_id not in self._documents:
                raise NotCachedException('Adding a fingerprint for which no document has been stored, the DB is in an inconsistent state.')

            if self._doc_ids is None:
                self._doc_ids = dict()
            self._doc_ids[doc_id] = fingerprint

    def get_bucket(self, bin_i, bucket_id):
        return self._fingerprints[bin_i][bucket_id]

    def add_document(self, doc, doc_id):
        if self.cache_documents:
            self._documents[doc_id] = doc

    def get_fingerprint(self, doc_id):
        if doc_id not in self._doc_ids:
            raise KeyError(f'Tried to retrieve fingerprint for non existent document id:{doc_id}.')
        return self._doc_ids[doc_id]

    def get_document(self, doc_id):
        if self.cache_documents:
            return self._documents[doc_id]
        else:
            raise NotCachedException('Can not retrieve document by ID when cache_documents=False.')


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