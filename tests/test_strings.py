# vim :set ts=4 sw=4 sts=4 et :
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
    print r.append("foo","bar")

def teardown_module(module):
    global pid
    os.kill(pid, signal.SIGKILL)
    print("Killed server.")


def test_append():
    eq_(r.set('test:key', 'value'),'OK')
    eq_(r.append('test:key', 'value'),10)
    eq_(r.get('test:key'),'valuevalue')
