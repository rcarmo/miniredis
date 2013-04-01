#!/usr/bin/env python
# encoding: utf-8
"""
Based on a minimalist Redis server originally written by Benjamin Pollack

First modified by Rui Carmo on 2013-03-12
Published under the MIT license.
"""

from gevent import monkey; monkey.patch_all()
import os, sys, logging
from redis.server import RedisServer

def main():
    m = RedisServer()
    try:
        m.run()
    except KeyboardInterrupt:
        m.stop()
    sys.exit(0)


if __name__ == '__main__':
    main()
