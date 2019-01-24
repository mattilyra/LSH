# -*- coding: utf-8 -*-
from __future__ import division

from typing import Set
import itertools
import logging
from warnings import warn

import numpy as np

from .backends import DictBackend, SqliteBackend

__author__ = "Matti Lyra"


class Cache(object):
    """LSH provides a way of determining the local neighbourhood of a document.

    Locality Sensitive Hashing relies on probabilistic guarantees of a hash
    function family to produce hash collisions for similar content. The
    implementation uses MinHash to produce those collisions and allows for fast
    deduplication of data sets without having to do all pairs comparisons.
    """

    def __init__(self, hasher, num_bands=10, backend='dict', cache_documents=False, **kwargs):
        """

        Parameters
        ----------
        hasher Hash function to use  (default: minhash)
        num_bands Number of bands to chop each fingerprint into (default: num_bands)
        backend Backend for storing seen fingerprints (default: DictBackend)
        cache_documents passed to the backend constructor
        kwargs
        """
        # each fingerprint is divided into n bins (bands) and duplicate
        # documents are computed only for documents that land in the same
        # bucket in one of the bins
        # bins[idx of band where docs may overlap][hash of fingerprint] ->
        # list of doc ids that have that fingerprint segment at that position
        if backend == 'dict':
            self.backend = DictBackend(num_bands, cache_documents, **kwargs)
        else:
            raise ValueError(f"Unkown backend 'backend'.")
        self.hasher = hasher
        msg = 'The number of seeds in the fingerprint must ' \
              'be divisible by the number of bands'
        assert hasher.num_seeds % num_bands == 0, msg
        self.band_width = hasher.num_seeds // num_bands
        self.num_bands = num_bands

        try:
            b = self.backend()
            b['test'] = 0
            _ = b['test']
        except KeyError as err:
            warn("It seems the backend you provided is not adding elements to it.")
            raise
        except Exception:
            pass

    def bins_(self, fingerprint):
        yield from enumerate(np.array_split(fingerprint, self.num_bands))

    def clear(self):
        self.backend.clear()
        self.hasher.fingerprint.cache_clear()

    def add_doc(self, doc, doc_id):
        if doc_id is None:
            raise ValueError(f'Document ID {doc_id} can not be None.')

        fingerprint = self.hasher.fingerprint(doc.encode('utf8'))
        self.add_fingerprint(fingerprint, doc_id)

    def add_fingerprint(self, fingerprint, doc_id):
        if isinstance(self.backend, SqliteBackend):
            bins_buckets = [(bin_i, hash(tuple(bucket))) for bin_i, bucket in self.bins_(fingerprint)]
            self.backend.add_fingerprint(bins_buckets, fingerprint, doc_id)
        else:
            for bin_i, bucket in self.bins_(fingerprint):
                bucket_id = hash(tuple(bucket))
                self.backend.add_fingerprint(bin_i, bucket_id, fingerprint, doc_id)

    def filter_candidates(self, candidate_id_pairs, min_jaccard):
        """Check a list of candidate ID pairs for approximate duplicates.

        This method only compares the fingerprints of the documents, not the exact shingle sets. While this is faster
        it is also less accurate.

        Parameters
        ----------
        candidate_id_pairs
        min_jaccard
        """
        logging.info('Computing Jaccard sim of %d pairs',
                     len(candidate_id_pairs))

        if min_jaccard is None:
            min_jaccard = 0

        for id1, id2 in candidate_id_pairs:
            # todo id1, id2 may not be contained in data
            f1 = self.backend.get_fingerprint(id1)
            f2 = self.backend.get_fingerprint(id2)
            jaccard = self.hasher.jaccard(f1, f2)
            if jaccard > min_jaccard:
                yield (id1, id2)
        # logging.info('Keeping %d/%d candidate duplicate pairs',
        #              len(res), len(candidate_id_pairs))
        # return res

    def remove_id(self, doc_id):
        fingerprint = self.backend.get_fingerprint(doc_id)
        for bin_i, bucket in self.bins_(fingerprint):
            bucket_id = hash(tuple(bucket))
            self.backend.remove_docid(bin_i, bucket_id, doc_id)

        self.backend.remove_fingerprint(doc_id)

    def remove_doc(self, doc):
        fingerprint = self.hasher.fingerprint(doc.encode('utf8'))
        doc_ids = {id for id, finger in self.backend._fingerprints.items()
                   if all(a == b for a, b in zip(finger, fingerprint))}
        for i in doc_ids:
            self.remove_id(i)

    def get_all_duplicates(self, min_jaccard=None):
        candidate_pairs = set()
        for bucket in self.backend.get_all_buckets():
            if len(bucket) > 1:
                pairs_ = set(itertools.combinations(bucket, r=2))
                candidate_pairs.update(pairs_)
        if min_jaccard is None:
            return candidate_pairs

        return self.filter_candidates(candidate_pairs, min_jaccard)

    def get_duplicates_of(self, doc:str=None, doc_id:int=None, min_jaccard:float=None) -> Set[int]:
        cleanup = False
        try:
            fingerprint = self.backend.get_fingerprint(doc_id)
        except KeyError:
            if doc is not None:
                # add document to the backend under a fake ID (-1)
                self.add_doc(doc, -1)
                cleanup = True
                doc_id = -1
                fingerprint = self.backend.get_fingerprint(-1)
            else:
                raise ValueError('Must provide a document or a known document id')

        def _filter(gen, dedup):
            """Private method for filtering the generator of tuples from self.filter_candidates
            to a list of document IDs"""
            for _, doc_id in gen:
                if doc_id in dedup:
                    continue
                else:
                    dedup.add(doc_id)
                    yield doc_id

        dedup = set()
        try:
            for bin_i, bucket in self.bins_(fingerprint):
                bucket_id = hash(tuple(bucket))
                bucket_ = self.backend.get_bucket(bin_i, bucket_id) - j[-1])
                for doc_id2 in bucket_:
                    yield from _filter(self.filter_candidates([(doc_id, doc_id2)], min_jaccard=min_jaccard), dedup)
        finally:
            # make sure we clean up after ourselves even if the generator is not consumed fully
            if cleanup:
                self.remove_id(-1)

    def filter_candidate_duplicates(self, fingerprint:tuple, candidates:Set[tuple], min_similarity:float=None):
        return {x for x in candidates
                if self.hasher.jaccard(fingerprint, x) > min_similarity}

    def is_duplicate(self, doc, doc_id=None, min_similarity=0.9):
        if self.backend.is_empty():
            return False

        if doc_id is not None and doc_id in self.backend._fingerprints:
            return True

        try:
            # if there is at least one duplicate, then the document is a duplicate
            # not need to check any further
            itr = self.get_duplicates_of(doc, doc_id=doc_id, min_jaccard=min_similarity)
            _ = next(itr)
        except StopIteration:
            return False
        else:
            return True
