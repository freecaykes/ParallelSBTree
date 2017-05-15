#!/usr/bin/env python
import os
import platform
import sys

from distutils.core import setup

# Determine platform being used.
system = platform.system()
USE_MAC = USE_WINDOWS = USE_X11 = False
if system == 'Darwin':
    USE_MAC = True
elif system == 'Windows' or system == 'Microsoft':
    USE_WINDOWS = True
else: # Default to X11
    USE_X11 = True

setup(name='ParallelSBTree',
      version='0.1',
      packages=['parallelsbtree','bintrees'],
      description='Function execution over large data stored in an AVLTree',
      author='freecaykes',
      url='https://github.com/freecaykes/parallelsbtree',
      platforms="OS Independent",
      license="MIT License",
     )
