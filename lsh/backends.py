import sqlite3


class DictBackend(object):
    def __init__(self, num_bands):
        self._data = [dict() for _ in num_bands]
        self._doc_ids = None

    def add(self, bin_id, bucket_id, fingerprint, doc_id=None):
        if bucket_id not in self._data:
            self._data[bucket_id] = set()
        self._data[bucket_id].add(fingerprint)

        if doc_id is not None:
            if self._doc_ids is None:
                self._doc_ids = dict()
            self._doc_ids[doc_id] = fingerprint


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