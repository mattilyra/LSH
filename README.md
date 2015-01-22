suckerpunch
===========

Sucker Punch is small Python library for performing near duplicate detection for text documents.

Like a sneaky adversary duplicate documents in a corpus tend to deal an unexpected blow (sucker punch)
to NLP pipelines. Locality sensitive hashing combined with shingling can be used to remove near
duplicates using probabilistic comparisons between documents.

Sucker punch uses the MurmurHash v3 library to create the document finger prints.

Cython is needed if you want regenerate the .cpp files for the hashing and shingling code. By default
the setup script uses the pregenerated .cpp sources, you can change this with the USE_CYTHON flag in
setup.py

NumPy is needed to run the code.

The MurmurHash3 library is distributed under the MIT license. More information https://code.google.com/smhasher


installation
============
> git clone https://github.com/mattilyra/suckerpunch suckerpunch
> cd suckerpunch
> python setup.py install
