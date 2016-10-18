#! /usr/bin/env python
from __future__ import print_function

from setuptools import setup, Extension
import numpy as np

USE_CYTHON = True

DISTNAME = 'lsh'
DESCRIPTION = 'A library for performing shingling and LSH for python.'

MAINTAINER = 'Matti Lyra'
MAINTAINER_EMAIL = 'm.lyra@sussex.ac.uk'
URL = 'https://github.com/mattilyra/lsh'
DOWNLOAD_URL = 'https://github.com/mattilyra/lsh'

VERSION = '0.2.0'

ext = '.pyx' if USE_CYTHON else '.cpp'
extensions = [Extension("lsh.cMinhash",
                        ["lsh/cMinhash{}".format(ext), 'lsh/MurmurHash3.cpp'],
                        include_dirs=[np.get_include()])]
if USE_CYTHON:
    from Cython.Build import cythonize

    extensions = cythonize(extensions)

install_deps = ['numpy', 'cython>=0.24.1']
test_deps = ['coverage>=4.0.3', 'pytest>=3.0', ]
setup(name=DISTNAME,
      version=VERSION,
      description=DESCRIPTION,
      author=MAINTAINER,
      author_email=MAINTAINER_EMAIL,
      url=URL,
      packages=['lsh'],
      ext_modules=extensions,
      install_requires=install_deps,
      tests_require=test_deps)
