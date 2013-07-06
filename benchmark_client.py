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
        c.select(1)
        seq = range(0,count)
        now = time.time()
        for i in seq:
            it = str(random.choice(seq))
            c.set(it, it)
            it = str(random.choice(seq))
            c.get(it)
        return count/(time.time() - now)

    p = Pool(4)
    print sum(p.map(timed,[10000,10000,10000,10000]))
    #print timed(10000)
