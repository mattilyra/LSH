# -*- coding: utf-8 -*-

from __future__ import division

__author__ = "Matti Lyra"

import re
import logging
from collections import defaultdict
import zlib

from .minhash import MinHasher

_logger = logging.getLogger(__name__)


class Cache(object):
    """LSH provides a way of determining the local neighbourhood of a document.

    Locality Sensitive Hashing relies on probabilistic guarantees of a hash
    function family to produce hash collisions for similar content. The
    implementation uses MinHash to produce those collisions and allows for fast
    deduplication of data sets without having to do all pairs comparisons.

    >>> lsh = Cache()
    >>> lsh.is_duplicate('This is a simple document')
    False
    >>> long_doc = 'A much longer document that contains lots of information\
    different words. The document produces many more shingles.'
    >>> lsh.is_duplicate(long_doc)
    False
    >>> lsh.is_duplicate('Not a duplicate document.')
    False
    >>> lsh.is_duplicate('This is a simple document')
    True
    >>> long_doc = long_doc.split()
    >>> long_doc = ' '.join([long_doc[0]] + long_doc[2:])
    >>> lsh.is_duplicate(long_doc)
    True"""
    def __init__(self, bands=10, rows=None, hasher=None, compress=0):
        if hasher is not None:
            self.hasher = hasher
        # each fingerprint is divided into n bins (bands) and duplicate
        # documents are computed only for documents that land in the same
        # bucket in one of the bins
        self.bins = [defaultdict(set) for _ in range(bands)]
        if rows is None:
            if hasher is None:
                raise RuntimeError('Rows and hasher can not both be None. \
 Either set rows explicitly or provide a hasher in which case the number rows \
 is calculated to be such that b*r == n')
            else:
                self.rows = hasher.seeds // bands
        else:
            self.rows = rows

        self.removed_articles = 0
        self.compress = compress

    def update(self, fingerprint, docid=None):
        for bin_i, head in enumerate(range(0, len(self.bins))):
            # take r length vectors of minhashes from band i
            head = head * self.rows
            if head > len(fingerprint):
                raise RuntimeError('The number of bands * rows exceeds the \
size of the fingerprint.')
            bucket = fingerprint[head:head + self.rows]
            bucket_id = hash(tuple(bucket))
            entry = (fingerprint, docid)
            self.bins[bin_i][bucket_id].add(entry)

    def _neighbours(self, fingerprint):
        bin_size = len(fingerprint) // len(self._bins)
        for bin_i, head in enumerate(range(0, len(fingerprint), bin_size)):
            # take r length vectors of minhashes from band i
            head = head * self.rows
            bucket = fingerprint[head:head + bin_size]
            bucket_id = hash(tuple(bucket))
            yield (n in self.bins[bin_i][bucket_id])

    def is_duplicate(self, article, min_similarity=0.65,
                     update=True, **kwargs):
        """Checks if a document is a duplicate of an already seen document.

        If the Jaccard similarity of two documents exceeds *min_similarity*
        those two documents are deemed to be duplicates of each other.

        The method returns a boolean indicating if the document is a
        duplicate.

        If a document is not a duplicate of anything in the cache it is added to
        the cache by default. This behaviour can be turned off with the *update*
        flag.
        """

        stripped_txt = article.replace('\n', ' ')
        stripped_txt = re.sub('[\.,;:\-\+=&*!?><\(\)]*', '', stripped_txt)
        stripped_txt = stripped_txt.lower()
        stripped_txt = stripped_txt.encode('utf8').strip()
        fingerprint = minhash(stripped_txt,
                              len(stripped_txt), self._seeds, 8)

        neighbours = self._neighbours(fingerprint)

        ngram = self._ngram
        end = len(stripped_txt) - ngram + 1
        doc_shingles = set([stripped_txt[head:head + ngram] for head in
                            range(0, end)])

        for neighbour in neighbours:
            neighbour_txt = zlib.decompress(neighbour)
            end = len(neighbour_txt) - ngram + 1
            neighbour_shingles = set([neighbour_txt[head:head + ngram]
                                      for head in range(0, end)])

            union = neighbour_shingles | doc_shingles
            intersection = neighbour_shingles & doc_shingles
            jaccard = len(intersection) / (len(union) + .0)
            if jaccard >= min_similarity:
                self.removed_articles += 1
                return True

        if update:
            self._update(fingerprint, stripped_txt)
        return False

if __name__ == '__main__':
    lsh = Cache()
    lsh.is_duplicate('A much longer document that contains lots of information. The document produces many more shingles.')
    lsh.is_duplicate('A longer document that contains lots of information. The document produces many more shingles.')

    import doctest
    doctest.testmod()
