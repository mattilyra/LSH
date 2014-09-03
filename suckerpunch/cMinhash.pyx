# distutils: language = c++
# distutils: sources = MurmurHash3.cpp

__author__ = "Matti Lyra"

cimport cython
from cython.view cimport array as cvarray
from libc.stdlib cimport malloc
from libc.stdint cimport uint32_t, int64_t
import numpy as np
cimport numpy as np

cdef extern from "MurmurHash3.h":
    void MurmurHash3_x64_128 (const void *key, int len, uint32_t seed, void *out) nogil

@cython.boundscheck(False) # turn of bounds-checking for entire function
def minhash(char* c_str,
            int strlen,
            np.ndarray[dtype=np.uint32_t, ndim=1] seeds not None,
            int char_ngram):

    cdef uint32_t num_seeds = len(seeds)
    cdef np.ndarray[np.int64_t, ndim=2] fingerprint = np.zeros((num_seeds, 2),
                                                               dtype=np.int64)

    cdef int64_t INT64_MAX = 9223372036854775807
    cdef int64_t hashes[2]
    cdef int64_t minhash[2]
    cdef int64_t [:, :] mem_view = fingerprint # memory view to the numpy array
    cdef uint32_t i, s
    with nogil:
        for s in range(num_seeds):
            minhash[0] = INT64_MAX
            minhash[1] = INT64_MAX
            for i in range(strlen-char_ngram+1):
                MurmurHash3_x64_128(c_str, char_ngram, seeds[s], hashes)

                if hashes[0] < minhash[0]:
                    minhash[0] = hashes[0]
                    minhash[1] = hashes[1]
                elif hashes[0] == minhash[0] and hashes[1] < minhash[1]:
                    minhash[0] = hashes[0]
                    minhash[1] = hashes[1]
                c_str += 1

            # store the current minhash
            mem_view[s][1] = minhash[1]
            mem_view[s][0] = minhash[0]

            # reset string pointer for next hash
            c_str -= strlen - char_ngram + 1

    return fingerprint
