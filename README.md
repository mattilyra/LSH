suckerpunch
===========

Sucker Punch is small Python library for performing near duplicate detection for text documents.

Like a sneaky adversary duplicate documents in a corpus tend to deal an unexpected blow (sucker punch)
to NLP pipelines. Locality sensitve hashing combined with shingling can be used to remove near
duplicates using a probabilistic comparisons between documents.

Sucker punch uses the MurmurHash v3 library to create the document finger prints. Cython is needed to
compile to hashing and shingling code, and the MurmurHash v3 header files should be in a place where
Cython can see them.
