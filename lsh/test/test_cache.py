import pytest
import numpy as np
from lsh.minhash import MinHasher

from lsh.cache import Cache


def test_hasher_json_serialisation(tmpdir):
    path = str(tmpdir.join("hasher.json"))

    hasher = MinHasher(seeds=100)
    hasher.to_json(path)
    hasher2 = MinHasher.from_json_file(path)

    doc = 'Once upon a time in a galaxy far far away and what not'
    np.testing.assert_array_equal(hasher.fingerprint(doc),
                                  hasher2.fingerprint(doc))


def test_cache_json_serialisation(tmpdir):
    path = str(tmpdir.join("cache.json"))
    hasher = MinHasher(100)
    lsh = Cache(hasher)

    # easy case- the bins array is empty
    lsh.to_json(path)
    lsh2 = Cache.from_json(path)

    # now add some data
    lsh.update("This is a document", 0)
    lsh2.update("This is a document", 0)

    lsh.to_json(path)
    lsh2 = Cache.from_json(path)

    lsh.update("The king of Denmark", 1)
    lsh2.update("The king of Denmark", 1)
    lsh.update("The queen of Zerg", 2)
    lsh2.update("The queen of Zerg", 2)

    lsh.to_json(path)
    lsh2 = Cache.from_json(path)


@pytest.mark.parametrize("char_ngram", [2, 3, 4, 5, 6])
@pytest.mark.parametrize("hashbytes", [4, 8])
# trick pytest into repeat all param combinations 5 times
@pytest.mark.parametrize("iter", range(5))
def test_cache(char_ngram, hashbytes, iter):
    hasher = MinHasher(seeds=200, char_ngram=char_ngram, hashbytes=hashbytes)
    lsh = Cache(hasher, num_bands=50)
    # very small band width => always find duplicates

    short_doc = 'This is a simple document'
    another_doc = 'Some text about animals.'
    long_doc = 'A much longer document that contains lots of information\
       different words. The document produces many more shingles.'

    assert not lsh.is_duplicate(short_doc)
    lsh.update(short_doc, 0)
    assert lsh.get_duplicates_of(short_doc) == {0}
    assert not lsh.is_duplicate(short_doc, doc_id=0)
    assert lsh.is_duplicate(short_doc)  # no id provided, compare by

    assert not lsh.is_duplicate(long_doc)
    lsh.update(long_doc, 1)
    lsh.update(another_doc, 2)

    # id is provided, so ignoree matches to self. the doc is therefore unique
    assert not lsh.is_duplicate(another_doc, doc_id=2)
    # w/o an id, the doc will match itself
    assert lsh.is_duplicate(another_doc)

    assert not lsh.is_duplicate(long_doc, doc_id=1)

    words = long_doc.split()
    long_doc_missing_word = ' '.join([words[0]] + words[2:])

    assert lsh.get_duplicates_of(long_doc_missing_word) == {1}
    assert lsh.is_duplicate(long_doc_missing_word)
    assert lsh.is_duplicate(long_doc + ' Word.')

    assert lsh.get_all_duplicates() == set()
    lsh.update(long_doc_missing_word, 3)
    assert lsh.get_all_duplicates() == {(1, 3)}

    lsh.update(long_doc_missing_word, 4)
    assert lsh.get_all_duplicates() == {(1, 3), (1, 4), (3, 4)}


# todo add 10 ever so slightly different documents to three caches
# check that hasher with low band_width finds more matches (over 50 runs)

def test_jaccard():
    hasher = MinHasher(seeds=200, char_ngram=2, hashbytes=4)
    assert hasher.jaccard("This is a doc", "This is a doc") == 1

    high_j = hasher.jaccard("This is a doc", "That is a doc")
    low_j = hasher.jaccard("This is a doc", "Cats in a tree")
    assert 0 < low_j < high_j < 1


@pytest.mark.parametrize("num_bands", [3, 6, 7, 9, 71, 99, 101])
def test_invalid_settings(num_bands):
    with pytest.raises(AssertionError):
        hasher = MinHasher(seeds=100)
        lsh = Cache(hasher, num_bands=num_bands)
        lsh.update('Hi', 1)
        lsh.get_duplicates_of('Hello')
