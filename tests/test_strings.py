# vim :set ts=4 sw=4 sts=4 et :
import os, sys, signal, time
from nose.tools import ok_, eq_, istest
import unittest

# Adjust path to import miniredis
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, project_root)

import miniredis.server
from miniredis.client import RedisClient

server_process = None
r = None
test_port = 6380 # Ensure consistent port with other tests if run together

def start_server():
    """Starts the Redis server in a background process."""
    from multiprocessing import Process
    def run_server_process():
        def sigterm_handler(signum, frame):
            sys.exit(0)
        signal.signal(signal.SIGTERM, sigterm_handler)
        server = None
        try:
            server = miniredis.server.RedisServer(port=test_port)
            server.run()
        except KeyboardInterrupt:
            pass
        finally:
            if server:
                server.stop()

    proc = Process(target=run_server_process, daemon=True)
    proc.start()
    time.sleep(0.5)
    if not proc.is_alive():
        raise RuntimeError("Server process failed to start.")
    print(f"Launched test server with pid {proc.pid} on port {test_port}.")
    return proc, test_port

def stop_server(proc):
    """Stops the Redis server process."""
    if proc and proc.is_alive():
        print(f"Terminating test server with pid {proc.pid}.")
        proc.terminate()
        proc.join(timeout=2)
        if proc.is_alive():
            print(f"Server process {proc.pid} did not terminate gracefully, killing.")
            proc.kill()
        print("Server stopped.")

def setup_module(module):
    global server_process, r
    try:
        server_process, _ = start_server() # Port is fixed
        r = RedisClient(port=test_port)
        r.flushdb()
    except Exception as e:
        print(f"Error during setup: {e}")
        if r: try: r.close(); except: pass
        if server_process: stop_server(server_process)
        raise

def teardown_module(module):
    global server_process, r
    if r: try: r.close(); except: pass
    if server_process: stop_server(server_process)


class TestStringCommands(unittest.TestCase):

    def setUp(self):
        # Optional: r.flushdb() if tests need isolation
        pass

    def test_append(self):
        """Test APPEND command"""
        # Key exists
        eq_(r.set('test:append:key1', 'value'), 'OK')
        eq_(r.append('test:append:key1', 'more'), 9) # Returns length after append
        eq_(r.get('test:append:key1'), b'valuemore')

        # Key does not exist
        eq_(r.append('test:append:key2', 'newvalue'), 8) # Creates key, returns length
        eq_(r.get('test:append:key2'), b'newvalue')

        # Append to non-string (should fail or be handled by server)
        # Assuming server returns error for wrong type
        r.lpush('test:append:list', 'item')
        with self.assertRaises(Exception, msg="APPEND on list should raise error") as cm:
            r.append('test:append:list', 'stuff')
        # Check if the error message indicates wrong type (adjust based on server.py)
        self.assertIn("Operation against a key holding the wrong kind of value", str(cm.exception))

    def test_incr_decr(self):
        """Test INCR and DECR commands"""
        eq_(r.set('test:counter', '10'), 'OK')
        eq_(r.incr('test:counter'), 11)
        eq_(r.get('test:counter'), b'11')
        eq_(r.incrby('test:counter', 5), 16)
        eq_(r.get('test:counter'), b'16')

        eq_(r.decr('test:counter'), 15)
        eq_(r.get('test:counter'), b'15')
        eq_(r.decrby('test:counter', 5), 10)
        eq_(r.get('test:counter'), b'10')

        # Non-existent key
        eq_(r.incr('test:newcounter'), 1)
        eq_(r.get('test:newcounter'), b'1')
        eq_(r.decr('test:newcounter2'), -1)
        eq_(r.get('test:newcounter2'), b'-1')

        # Error cases
        r.set('test:notint', 'hello')
        with self.assertRaises(Exception) as cm:
            r.incr('test:notint')
        self.assertIn("value is not an integer", str(cm.exception))

        with self.assertRaises(Exception) as cm:
            r.decr('test:notint')
        self.assertIn("value is not an integer", str(cm.exception))

        # Ensure large numbers are handled (if server supports)
        # large_num_str = str(2**63 - 1) # Max 64-bit signed int
        # r.set('test:largecounter', large_num_str)
        # eq_(r.incr('test:largecounter'), 2**63) # This might overflow depending on Python/server int size

    def test_getset(self):
        """Test GETSET command"""
        # Key exists
        r.set('test:getset:key1', 'oldvalue')
        old = r.getset('test:getset:key1', 'newvalue')
        eq_(old, b'oldvalue')
        eq_(r.get('test:getset:key1'), b'newvalue')

        # Key does not exist
        old = r.getset('test:getset:key2', 'firstvalue')
        eq_(old, None) # Returns nil (None for client) when key didn't exist
        eq_(r.get('test:getset:key2'), b'firstvalue')

        # GETSET on non-string type
        r.lpush('test:getset:list', 'item')
        with self.assertRaises(Exception) as cm:
            r.getset('test:getset:list', 'new')
        self.assertIn("Operation against a key holding the wrong kind of value", str(cm.exception))

    def test_mget(self):
        """Test MGET command"""
        r.set('test:mget:key1', 'val1')
        r.set('test:mget:key2', 'val2')
        r.lpush('test:mget:list', 'item') # A non-string key

        results = r.mget('test:mget:key1', 'test:mget:nonexistent', 'test:mget:key2', 'test:mget:list')
        expected = [b'val1', None, b'val2', None] # Expect None for non-existent and wrong type
        eq_(results, expected)

        # Empty list
        eq_(r.mget(), [])

    def test_setnx(self):
        """Test SETNX command"""
        # Key does not exist
        eq_(r.setnx('test:setnx:key1', 'value1'), 1)
        eq_(r.get('test:setnx:key1'), b'value1')

        # Key exists
        eq_(r.setnx('test:setnx:key1', 'value2'), 0)
        eq_(r.get('test:setnx:key1'), b'value1') # Value should not change

    def test_setex(self):
        """Test SETEX command"""
        eq_(r.setex('test:setex:key1', 2, 'value'), 'OK')
        eq_(r.get('test:setex:key1'), b'value')
        ttl = r.ttl('test:setex:key1')
        ok_(ttl >= 1 and ttl <= 2, f"TTL ({ttl}) not within expected range [1, 2]")
        time.sleep(2.1)
        eq_(r.get('test:setex:key1'), None)
        eq_(r.ttl('test:setex:key1'), -2)

        # Invalid TTL
        with self.assertRaises(Exception) as cm:
            r.setex('test:setex:key2', 'notanumber', 'value')
        self.assertIn("value is not an integer", str(cm.exception))

        with self.assertRaises(Exception) as cm:
            r.setex('test:setex:key3', -10, 'value') # Negative TTL is invalid for SETEX
        # Check specific error if server provides one, otherwise generic Exception
        # self.assertIn("invalid expire time", str(cm.exception))


# Allow running with 'python test_strings.py'
if __name__ == '__main__':
    import nose
    nose.runmodule()
