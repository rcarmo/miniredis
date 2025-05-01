#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Copyright (c) 2013, Rui Carmo
Description: Experimental Cython compile script
License: MIT (see LICENSE.md for details)
"""

import os, sys
from setuptools import setup, find_packages  # Use setuptools and find_packages

setup(
    name="miniredis",
    version="0.1.0",
    packages=find_packages(exclude=["tests*"]),  # Automatically find packages
    setup_requires=['pytest-runner'],  # Update test runner
    tests_require=['pytest'],  # Update test dependencies
    author="Rui Carmo",
    author_email="rui@example.com",  # Placeholder email
    description="Pure Python Redis protocol subset implementation",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    license="MIT",
    keywords="redis server mock test",
    url="https://github.com/rcarmo/miniredis",
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
    python_requires=">=3.7",
)
