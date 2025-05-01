# vim :set ts=4 sw=4 sts=4 et :
import os, sys, signal, time
from nose.tools import ok_, eq_, istest
import unittest  # Use unittest for potential future expansion

# Adjust path to import miniredis
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, project_root)

import miniredis.server
from miniredis.client import RedisClient

pid = None
r = None
server_process = None  # Use multiprocessing.Process for better control


def start_server():
    """Starts the Redis server in a background process."""
    # Use a different port for testing to avoid conflicts
    test_port = 6380
    # Use multiprocessing Process for cleaner start/stop
    from multiprocessing import Process

    def run_server_process():
        # Ensure server stops on interrupt within the process
        def sigterm_handler(signum, frame):
            sys.exit(0)

        signal.signal(signal.SIGTERM, sigterm_handler)
        try:
            server = miniredis.server.RedisServer(port=test_port)
            server.run()
        except KeyboardInterrupt:
            pass  # Expected on shutdown
        finally:
            if server:
                server.stop()

    proc = Process(target=run_server_process, daemon=True)
    proc.start()
    # Allow time for server to start
    time.sleep(0.5)  # Reduced sleep time
    if not proc.is_alive():
        raise RuntimeError("Server process failed to start.")
    print(f"Launched test server with pid {proc.pid} on port {test_port}.")
    return proc, test_port


def stop_server(proc):
    """Stops the Redis server process."""
    if proc and proc.is_alive():
        print(f"Terminating test server with pid {proc.pid}.")
        proc.terminate()  # Send SIGTERM
        proc.join(timeout=2)  # Wait for process to exit
        if proc.is_alive():
            print(f"Server process {proc.pid} did not terminate gracefully, killing.")
            proc.kill()  # Force kill if terminate fails
        print("Server stopped.")


def setup_module(module):
    global server_process, r, test_port
    try:
        server_process, test_port = start_server()
        r = RedisClient(port=test_port)
        # Flush DB before tests
        r.flushdb()
    except Exception as e:
        print(f"Error during setup: {e}")
        # Ensure cleanup if setup fails partially
        if r:
            try:
                r.close()  # Close client connection if open
            except:
                pass
        if server_process:
            stop_server(server_process)
        raise  # Re-raise exception to fail the test suite


def teardown_module(module):
    global server_process, r
    if r:
        try:
            r.close()
        except:
            pass
    if server_process:
        stop_server(server_process)


# Use a class for tests for better structure (optional but good practice)
class TestKeysCommands(unittest.TestCase):
    def setUp(self):
        # Runs before each test method
        # Ensure clean state if needed, though setup_module handles initial flush
        # r.flushdb() # Uncomment if tests interfere with each other
        pass

    def tearDown(self):
        # Runs after each test method
        pass

    def test_put_get(self):
        """Test basic SET and GET"""
        eq_(r.set("test:key", "value"), "OK")
        # GET returns bytes, need to decode
        result = r.get("test:key")
        eq_(result, b"value")
        eq_(result.decode("utf-8"), "value")

    def test_get_nonexistent(self):
        """Test GET on a non-existent key"""
        eq_(r.get("test:notakey"), None)

    def test_del(self):
        """Test DEL command"""
        r.set("test:keydel1", "value1")
        r.set("test:keydel2", "value2")
        r.set("test:keydel3", "value3")
        # single key
        eq_(r.delete("test:keydel1"), 1)
        eq_(r.get("test:keydel1"), None)
        # multiple keys
        eq_(r.delete("test:keydel2", "test:keydel3"), 2)
        eq_(r.get("test:keydel2"), None)
        eq_(r.get("test:keydel3"), None)
        # non-existent key
        eq_(r.delete("test:notthere"), 0)

    def test_exists(self):
        """Test EXISTS command"""
        r.set("test:keyexists", "value")
        eq_(r.exists("test:keyexists"), 1)
        eq_(r.exists("test:notthere"), 0)

    def test_expire_ttl(self):
        """Test EXPIRE and TTL commands"""
        r.set("test:keyexpire", "value")
        # missing key
        eq_(r.expire("test:notthere", 2), 0)
        # valid setting
        eq_(r.expire("test:keyexpire", 2), 1)
        # TTL should be close to 2 (allow for slight delay)
        ttl = r.ttl("test:keyexpire")
        ok_(ttl >= 1 and ttl <= 2, f"TTL ({ttl}) not within expected range [1, 2]")
        time.sleep(2.1)
        eq_(r.ttl("test:keyexpire"), -2)  # Should be expired (-2)
        eq_(r.exists("test:keyexpire"), 0)

        # reset ttl with SET
        r.set("test:keyexpire_reset", "value")
        eq_(r.expire("test:keyexpire_reset", 5), 1)
        eq_(r.ttl("test:keyexpire_reset") > 0, True)
        eq_(r.set("test:keyexpire_reset", "newvalue"), "OK")
        eq_(r.ttl("test:keyexpire_reset"), -1)  # SET should remove TTL

    def test_expireat_pttl(self):
        """Test EXPIREAT and PTTL commands"""
        r.set("test:keyexpireat", "value")
        # missing key
        at_ts = int(time.time() + 2)
        eq_(r.expireat("test:notthere_at", at_ts), 0)
        # valid setting
        at_ts = int(time.time() + 2)
        eq_(r.expireat("test:keyexpireat", at_ts), 1)
        # PTTL should be close to 2000ms
        pttl = r.pttl("test:keyexpireat")
        ok_(
            pttl >= 1000 and pttl <= 2000,
            f"PTTL ({pttl}) not within expected range [1000, 2000]",
        )
        time.sleep(2.1)
        eq_(r.pttl("test:keyexpireat"), -2)  # Should be expired (-2)
        eq_(r.exists("test:keyexpireat"), 0)

        # reset ttl with SET
        r.set("test:keyexpireat_reset", "value")
        at_ts = int(time.time() + 5)
        eq_(r.expireat("test:keyexpireat_reset", at_ts), 1)
        ok_(r.pttl("test:keyexpireat_reset") > 0, True)
        eq_(r.set("test:keyexpireat_reset", "newvalue"), "OK")
        eq_(r.pttl("test:keyexpireat_reset"), -1)  # SET should remove TTL

    def test_keys(self):
        """Test KEYS command"""
        # Clear previous keys potentially matching pattern
        r.flushdb()
        # place test keys
        r.set("test:keys:key1", "value")
        r.set("test:keys:key2", "value")
        r.set("other:keys:key3", "value")
        # KEYS returns list of bytes
        result_bytes = r.keys("test:keys:*")
        result_strings = sorted([k.decode("utf-8") for k in result_bytes])
        eq_(result_strings, ["test:keys:key1", "test:keys:key2"])

        result_bytes = r.keys("*:key?")
        result_strings = sorted([k.decode("utf-8") for k in result_bytes])
        eq_(result_strings, ["other:keys:key3", "test:keys:key1", "test:keys:key2"])

        result_bytes = r.keys("*nomatch*")
        eq_(result_bytes, [])


# Allow running with 'python test_keys.py'
if __name__ == "__main__":
    import nose

    nose.runmodule()
