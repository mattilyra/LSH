# -*- coding: utf-8 -*-
from __future__ import division

import json
from collections import defaultdict
import itertools
import logging
from copy import deepcopy

import numpy as np
from lsh.minhash import MinHasher

__author__ = "Matti Lyra"


class Cache(object):
    """LSH provides a way of determining the local neighbourhood of a document.

    Locality Sensitive Hashing relies on probabilistic guarantees of a hash
    function family to produce hash collisions for similar content. The
    implementation uses MinHash to produce those collisions and allows for fast
    deduplication of data sets without having to do all pairs comparisons.
    """

    def __init__(self, hasher, num_bands=10, **kwargs):
        # each fingerprint is divided into n bins (bands) and duplicate
        # documents are computed only for documents that land in the same
        # bucket in one of the bins
        # bins[idx of band where docs may overlap][hash of fingerprint] ->
        # list of doc ids that have that fingerprint segment at that position
        self.bins = [defaultdict(set) for _ in range(num_bands)]
        self.hasher = hasher
        msg = 'The number of seeds in the fingerprint must ' \
              'be divisible by the number of bands'
        assert hasher.num_seeds % num_bands == 0, msg
        self.band_width = hasher.num_seeds // num_bands
        self.num_bands = num_bands

        self.seen_ids = set()

    def bins_(self, fingerprint):
        yield from enumerate(np.array_split(fingerprint, self.num_bands))

    def clear(self):
        self.bins = [defaultdict(set) for _ in range(self.num_bands)]
        self.hasher.fingerprint.cache_clear()

    def to_json(self, path):
        with open(path, 'w') as outf:
            json.dump(self.jsonable(), outf)

    @staticmethod
    def from_json(path):
        with open(path) as inf:
            data = json.load(inf)

        cache = Cache(MinHasher.from_json_str(data.pop('hasher')),
                      **data)
        bins = []
        for bin in data['bins']:
            b1 = defaultdict(set)
            b1.update({k: set(v) for k, v in bin.items()})
            bins.append(b1)
        cache.bins = bins
        return cache

    def jsonable(self):
        d = deepcopy(self.__dict__)
        d['hasher'] = d['hasher'].jsonable()
        d['seen_ids'] = list(d['seen_ids'])
        bins = []
        for bin in self.bins:
            # bin is a defaultdict(int->set[int])
            b1 = {k: list(v) for k, v in bin.items()}
            bins.append(b1)
        d['bins'] = bins
        return d

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

    def filter_candidates(self, candidates, min_jaccard=0, data=dict()):
        if min_jaccard and data:
            logging.info('Computing Jaccard sim of %d pairs',
                         len(candidates))
            res = set()
            for doc1, doc2 in candidates:
                # todo doc1, doc2 may not be contained in data
                jaccard = self.hasher.jaccard(data[doc1], data[doc2])
                if jaccard > min_jaccard:
                    res.add((doc1, doc2))
            logging.info('Keeping %d/%d candidate duplicates',
                         len(res), len(candidates))
            return res
        else:
            return candidates

    def get_all_duplicates(self, min_jaccard=0, data=dict()):
        candidate_pairs = set()
        for b in self.bins:
            for bucket_id in b:
                if len(b[bucket_id]) > 1:
                    pairs_ = set(itertools.combinations(b[bucket_id], r=2))
                    candidate_pairs.update(pairs_)
        return self.filter_candidates(candidate_pairs,
                                      min_jaccard=min_jaccard,
                                      data=data)

    def get_duplicates_of(self, doc, min_jaccard=0, data=dict()):
        fingerprint = self.hasher.fingerprint(doc.encode('utf8'))
        candidates = set()
        for bin_i, bucket in self.bins_(fingerprint):
            bucket_id = hash(tuple(bucket))
            candidates.update(self.bins[bin_i][bucket_id])
        return self.filter_candidates(candidates,
                                      min_jaccard=min_jaccard,
                                      data=data)

    def is_duplicate(self, doc, doc_id=None):
        if doc_id is not None and doc_id in self.seen_ids:
            return False

        return len(self.get_duplicates_of(doc) - {doc_id}) > 0
