#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Copyright (c) 2013, Rui Carmo
Description: Experimental Cython compile script
License: MIT (see LICENSE.md for details)
"""

import os, sys
from distutils.core import setup
from distutils.extension import Extension
from Cython.Distutils import build_ext
from Cython.Build import cythonize
from glob import glob


try:
    from Cython.Distutils import build_ext
except:
    print "You don't seem to have Cython installed"
    sys.exit(1)

def scandir(dir, files=[]):
    for file in os.listdir(dir):
        path = os.path.join(dir, file)
        if os.path.isfile(path) and path.endswith(".py"):
            files.append(path.replace(os.path.sep, ".")[:-3])
        elif os.path.isdir(path):
            scandir(path, files)
    return files
    
def makeExtension(extName):
    extPath = extName.replace(".", os.path.sep)+".py"
    return Extension(
        extName,
        [extPath],
        include_dirs = ["."],
        extra_compile_args = ["-O3", "-Wall"],
        extra_link_args = ['-g'],
        libraries = [],
    )    

extNames = scandir("redis")
extensions = [makeExtension(name) for name in extNames]

setup(
    name = "redis",
    packages = "redis",
    ext_modules=extensions,
    cmdclass = {'build_ext': build_ext},
)

