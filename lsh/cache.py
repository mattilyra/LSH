# -*- coding: utf-8 -*-
from __future__ import division

from collections import defaultdict
import itertools
import logging

__author__ = "Matti Lyra"

class Cache(object):
    """LSH provides a way of determining the local neighbourhood of a document.

    Locality Sensitive Hashing relies on probabilistic guarantees of a hash
    function family to produce hash collisions for similar content. The
    implementation uses MinHash to produce those collisions and allows for fast
    deduplication of data sets without having to do all pairs comparisons.
    """

    def __init__(self, num_bands=10, band_width=None, hasher=None, compress=0):
        if hasher is not None:
            self.hasher = hasher
        # each fingerprint is divided into n bins (bands) and duplicate
        # documents are computed only for documents that land in the same
        # bucket in one of the bins
        # bins[idx of band where docs may overlap][hash of fingerprint] ->
        # list of doc ids that have that fingerprint segment at that position
        self.bins = [defaultdict(set) for _ in range(num_bands)]
        if band_width is None:
            if hasher is None:
                raise RuntimeError(
                    'band_width and hasher can not both be None. Either set '
                    'rows explicitly or provide a hasher in which case the '
                    'number rows  is calculated to be such that b*r == n')
            else:
                self.band_width = hasher.seeds // num_bands
        else:
            self.band_width = band_width

        self.removed_articles = 0
        self.compress = compress

        self.seen_ids = set()

    def bins_(self, fingerprint):
        # todo can be replaced by numpy.array_split()
        for bin_i, idx in enumerate(range(0, len(self.bins))):
            # take r length vectors of minhashes from band i
            idx = idx * self.band_width
            if idx > len(fingerprint):
                raise RuntimeError('The number of bands * rows exceeds the '
                                   'size of the fingerprint.')
            bucket = fingerprint[idx:idx + self.band_width]
            yield bin_i, bucket

    def update(self, doc, doc_id):
        fingerprint = self.hasher.fingerprint(doc.encode('utf8'))

        if doc_id is not None and doc_id in self.seen_ids:
            # todo is this a problem? should we refuse to add it?
            logging.warning('Duplicate id %d', doc_id)
        self.seen_ids.add(doc_id)

        for bin_i, bucket in self.bins_(fingerprint):
            # todo? use murmur hash here? faster?
            bucket_id = hash(tuple(bucket))
            self.bins[bin_i][bucket_id].add(doc_id)

    def get_all_duplicates(self):
        candidate_pairs = set()
        for b in self.bins:
            for bucket_id in b:
                if len(b[bucket_id]) > 1:
                    pairs_ = set(itertools.combinations(b[bucket_id], r=2))
                    candidate_pairs.update(pairs_)
        return candidate_pairs

    def get_duplicates_of(self, doc):
        fingerprint = self.hasher.fingerprint(doc.encode('utf8'))
        duplicates = set()
        for bin_i, bucket in self.bins_(fingerprint):
            bucket_id = hash(tuple(bucket))
            duplicates.update(self.bins[bin_i][bucket_id])
        return duplicates

    def is_duplicate(self, doc, doc_id=None):
        if doc_id is not None and doc_id in self.seen_ids:
            return False

        return len(self.get_duplicates_of(doc) - {doc_id}) > 0
