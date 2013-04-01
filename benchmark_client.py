#!/usr/bin/env python
# encoding: utf-8
"""
Based on a minimalist Redis client originally written by Andrew Rowls

Created by Rui Carmo on 2013-03-12
Published under the MIT license.
"""

import os, sys, logging
from redis.client import RedisClient
from multiprocessing import Pool
import time
import random

log = logging.getLogger()

if __name__=='__main__':
    def timed(count):
        c = RedisClient()
        c.select(0)
        seq = range(0,10000)
        for i in seq:
            c.set(random.choice(seq),'bar' * random.choice(seq))
        now = time.time()
        for i in range(0,count):
            c.get(random.choice(seq))
        return time.time() - now

    p = Pool(2)
    print p.map(timed,[10000,10000,10000,10000])
