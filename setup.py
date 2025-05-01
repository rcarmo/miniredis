#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Copyright (c) 2013, Rui Carmo
Description: Experimental Cython compile script
License: MIT (see LICENSE.md for details)
"""

import os, sys
from setuptools import setup, Extension  # Use setuptools
from Cython.Build import cythonize
from glob import glob

# Check for Cython
try:
    from Cython.Build import cythonize
except ImportError:
    print("You don't seem to have Cython installed. Please install it.")
    print("pip install Cython")
    sys.exit(1)


def scandir(dir, files=None):
    if files is None:
        files = []
    for item in os.listdir(dir):
        path = os.path.join(dir, item)
        if os.path.isfile(path) and path.endswith(".py"):
            # Convert path to module notation
            module_path = path.replace(os.path.sep, ".")[:-3]
            files.append(module_path)
        elif os.path.isdir(path):
            # Recurse into subdirectories
            scandir(path, files)
    return files


def makeExtension(extName):
    extPath = extName.replace(".", os.path.sep) + ".py"
    return Extension(
        extName,
        [extPath],
        include_dirs=["."],  # Include current directory
    )


# Find modules to Cythonize
ext_package = "miniredis"
extNames = scandir(ext_package)

# Filter out __init__ if it doesn't need compiling or causes issues
extNames = [name for name in extNames if not name.endswith(".__init__")]

# Create Extension objects
extensions = [makeExtension(name) for name in extNames]

setup(
    name="miniredis",
    version="0.1.0",  # Add a version
    packages=[ext_package],  # Specify the package directory
    ext_modules=cythonize(
        extensions, compiler_directives={"language_level": "3"}
    ),  # Cythonize
    test_suite="nose.collector",  # Specify test suite runner
    tests_require=["nose>=1.0"],  # Specify test dependencies
    # Add other metadata
    author="Rui Carmo",
    author_email="your_email@example.com",  # Add email
    description="Pure Python Redis protocol subset implementation",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    license="MIT",
    keywords="redis server mock test",
    url="https://github.com/rcarmo/miniredis",  # Add URL
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Topic :: Software Development :: Testing",
        "Topic :: Database :: Front-Ends",
    ],
    python_requires=">=3.7",  # Specify minimum Python version
)
