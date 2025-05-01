# vim :set ts=4 sw=4 sts=4 et :
import os
import sys
import time
import pytest
from multiprocessing import Process
from typing import Generator

# Changed from relative import to absolute import
from tests.helpers import start_server, stop_server
from miniredis.client import RedisClient

@pytest.fixture(scope="module")
def redis_client() -> Generator[RedisClient, None, None]:
    """Pytest fixture to start/stop the miniredis server and provide a client."""
    server_process: Process | None = None
    r_client: RedisClient | None = None
    try:
        server_process, test_port = start_server()
        r_client = RedisClient(port=test_port)
        r_client.flushdb() # Flush DB before tests start
        yield r_client # Provide the client to the tests
    except Exception as e:
        print(f"Error during fixture setup in test_keys: {e}")
        pytest.fail(f"Fixture setup failed: {e}") # Fail tests if fixture fails
    finally:
        # Teardown: Stop client and server
        print("Tearing down test_keys fixture...")
        if r_client:
            try:
                r_client.close()
                print("Redis client closed.")
            except Exception as e:
                print(f"Error closing redis client: {e}")
        if server_process:
            stop_server(server_process)
        print("Fixture teardown complete.")

class TestKeysCommands:

    def test_put_get(self, redis_client: RedisClient):
        """Test basic SET and GET"""
        r = redis_client
        assert r.set("test:key", "value") == "OK"
        result = r.get("test:key")
        assert result == b"value"
        assert result.decode("utf-8") == "value"

    def test_get_nonexistent(self, redis_client: RedisClient):
        """Test GET on a non-existent key"""
        r = redis_client
        assert r.get("test:notakey") is None

    def test_del(self, redis_client: RedisClient):
        """Test DEL command"""
        r = redis_client
        r.set("test:keydel1", "value1")
        r.set("test:keydel2", "value2")
        r.set("test:keydel3", "value3")
        # single key
        assert r.delete("test:keydel1") == 1
        assert r.get("test:keydel1") is None
        # multiple keys
        assert r.delete("test:keydel2", "test:keydel3") == 2
        assert r.get("test:keydel2") is None
        assert r.get("test:keydel3") is None
        # non-existent key
        assert r.delete("test:notthere") == 0

    def test_exists(self, redis_client: RedisClient):
        """Test EXISTS command"""
        r = redis_client
        r.set("test:keyexists", "value")
        assert r.exists("test:keyexists") == 1
        assert r.exists("test:notthere") == 0

    def test_expire_ttl(self, redis_client: RedisClient):
        """Test EXPIRE and TTL commands"""
        r = redis_client
        r.set("test:keyexpire", "value")
        # missing key
        assert r.expire("test:notthere", 2) == 0
        # valid setting
        assert r.expire("test:keyexpire", 2) == 1
        # TTL should be close to 2 (allow for slight delay)
        ttl = r.ttl("test:keyexpire")
        assert 1 <= ttl <= 2, f"TTL ({ttl}) not within expected range [1, 2]"
        time.sleep(2.1)
        assert r.ttl("test:keyexpire") == -2  # Should be expired (-2)
        assert r.exists("test:keyexpire") == 0

        # reset ttl with SET
        r.set("test:keyexpire_reset", "value")
        assert r.expire("test:keyexpire_reset", 5) == 1
        assert r.ttl("test:keyexpire_reset") > 0
        assert r.set("test:keyexpire_reset", "newvalue") == "OK"
        assert r.ttl("test:keyexpire_reset") == -1  # SET should remove TTL

    def test_expireat_pttl(self, redis_client: RedisClient):
        """Test EXPIREAT and PTTL commands"""
        r = redis_client
        r.set("test:keyexpireat", "value")
        # missing key
        at_ts = int(time.time() + 2)
        assert r.expireat("test:notthere_at", at_ts) == 0
        # valid setting
        at_ts = int(time.time() + 2)
        assert r.expireat("test:keyexpireat", at_ts) == 1
        # PTTL should be close to 2000ms
        pttl = r.pttl("test:keyexpireat")
        assert (
            1000 <= pttl <= 2000
        ), f"PTTL ({pttl}) not within expected range [1000, 2000]"
        time.sleep(2.1)
        assert r.pttl("test:keyexpireat") == -2  # Should be expired (-2)
        assert r.exists("test:keyexpireat") == 0

        # reset ttl with SET
        r.set("test:keyexpireat_reset", "value")
        at_ts = int(time.time() + 5)
        assert r.expireat("test:keyexpireat_reset", at_ts) == 1
        assert r.pttl("test:keyexpireat_reset") > 0
        assert r.set("test:keyexpireat_reset", "newvalue") == "OK"
        assert r.pttl("test:keyexpireat_reset") == -1  # SET should remove TTL

    def test_keys(self, redis_client: RedisClient):
        """Test KEYS command"""
        r = redis_client
        # Clear previous keys potentially matching pattern
        r.flushdb()
        # place test keys
        r.set("test:keys:key1", "value")
        r.set("test:keys:key2", "value")
        r.set("other:keys:key3", "value")
        # KEYS returns list of bytes
        result_bytes = r.keys("test:keys:*")
        result_strings = sorted([k.decode("utf-8") for k in result_bytes])
        assert result_strings == ["test:keys:key1", "test:keys:key2"]

        result_bytes = r.keys("*:key?")
        result_strings = sorted([k.decode("utf-8") for k in result_bytes])
        assert result_strings == ["other:keys:key3", "test:keys:key1", "test:keys:key2"]

        result_bytes = r.keys("*nomatch*")
        assert result_bytes == []
