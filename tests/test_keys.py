import os, sys, signal, time
from nose.tools import ok_, eq_, istest

sys.path.append('..')

import miniredis.server
from miniredis.client import RedisClient

pid = None

def setup_module():
	global pid
	pid = miniredis.server.fork()
	print pid
	time.sleep(1)

def teardown_module():
	global pid
	os.kill(pid, signal.SIGKILL)


r = RedisClient()

def test_put():
    eq_(r.set('test:key', 'value'),'OK')

def test_get():
	eq_(r.get('test:key'),'value')

def test_del():
	eq_(r.delete('test:key'),1)
	eq_(r.get('test:key'),None)
	r.set('test:key1', 'value')
	r.set('test:key2', 'value')
	eq_(r.delete('test:key1', 'test:key2'),2)
