#!/usr/bin/env python
# encoding: utf-8
"""
First modified by Rui Carmo on 2013-03-12
Published under the MIT license.
"""

import os, sys, logging
from miniredis.server import RedisServer

#logging.basicConfig(level=logging.DEBUG)

def main():
    m = RedisServer()
    try:
        m.run()
    except KeyboardInterrupt:
        m.stop()
    sys.exit(0)


if __name__ == '__main__':
    main()
