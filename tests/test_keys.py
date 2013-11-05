import os, sys, signal, time
from nose.tools import ok_, eq_, istest

sys.path.append('..')

import miniredis.server
from miniredis.client import RedisClient

pid = None

def setup():
	global pid
	pid = miniredis.server.fork()

def teardown():
	global pid
	os.kill(pid, signal.SIGKILL)



r = RedisClient()

def test_put():
    eq_(r.put('test:key', 'value'),None)

