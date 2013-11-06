import os, sys, signal, time
from nose.tools import ok_, eq_, istest

sys.path.append('..')

import miniredis.server
from miniredis.client import RedisClient

pid = None
r = None

def setup_module(module):
	global pid, r
	pid = miniredis.server.fork()
	print("Launched server with pid %d." % pid)
	time.sleep(1)
	r = RedisClient()

def teardown_module(module):
	global pid
	os.kill(pid, signal.SIGKILL)
	print("Killed server.")


def test_put():
    eq_(r.set('test:key', 'value'),'OK')

def test_get():
	eq_(r.get('test:key'),'value')

def test_del():
	# single key
	eq_(r.delete('test:key'), 1)
	eq_(r.get('test:key'),None)
	# multiple keys
	r.set('test:key1', 'value')
	r.set('test:key2', 'value')
	eq_(r.delete('test:key1', 'test:key2'), 2)

def test_dump():
	eq_(r.set('test:key','value'), 'OK')
	eq_(r.dump('test:key'),'value')

def test_exists():
	eq_(r.exists('test:key'), 1)
	eq_(r.exists('test:notthere'), 0)

def test_expire():
	# missing key
	eq_(r.expire('test:notthere', 2), 0)
	# valid setting
	eq_(r.expire('test:key', 2), 1)
	eq_(r.ttl('test:key'), 2)
	# reset ttl
	eq_(r.set('test:key','value'), 'OK')
	eq_(r.ttl('test:key'), -1)

def test_expireat():
	# missing key
	at = int(time.time() + 2)
	eq_(r.expireat('test:notthere', at), 0)
	# valid setting
	at = int(time.time() + 2)
	eq_(r.expireat('test:key', at), 1)
	eq_(r.ttl('test:key'), 2)
	# reset ttl
	eq_(r.set('test:key','value'), 'OK')
	eq_(r.ttl('test:key'), -1)

