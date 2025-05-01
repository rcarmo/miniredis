# vim :set ts=4 sw=4 sts=4 et :
import os
import sys
import time
import pytest # Changed from unittest
from multiprocessing import Process
from typing import Generator, Any

# Import from shared helper
from .helpers import start_server, stop_server
from miniredis.client import RedisClient

# Use the same fixture as test_keys.py if tests can share the same server instance
# If they need independent servers, define a similar fixture here or adjust scope.
# For simplicity, let's assume they can share the module-scoped server.
# If you defined the fixture in conftest.py, you wouldn't need to import it here.
from .test_keys import redis_client # Reuse fixture from test_keys

# No longer need unittest.TestCase
class TestStringCommands:

    # No longer need setUp with module-scoped fixture
    # def setUp(self):
    #     pass

    def test_append(self, redis_client: RedisClient):
        """Test APPEND command"""
        r = redis_client
        # Key exists
        assert r.set('test:append:key1', 'value') == 'OK'
        assert r.append('test:append:key1', 'more') == 9 # Returns length after append
        assert r.get('test:append:key1') == b'valuemore'

        # Key does not exist
        assert r.append('test:append:key2', 'newvalue') == 8 # Creates key, returns length
        assert r.get('test:append:key2') == b'newvalue'

        # Append to non-string (should fail or be handled by server)
        r.lpush('test:append:list', 'item')
        with pytest.raises(Exception, match="Operation against a key holding the wrong kind of value"):
            r.append('test:append:list', 'stuff')

    def test_incr_decr(self, redis_client: RedisClient):
        """Test INCR and DECR commands"""
        r = redis_client
        assert r.set('test:counter', '10') == 'OK'
        assert r.incr('test:counter') == 11
        assert r.get('test:counter') == b'11'
        assert r.incrby('test:counter', 5) == 16
        assert r.get('test:counter') == b'16'

        assert r.decr('test:counter') == 15
        assert r.get('test:counter') == b'15'
        assert r.decrby('test:counter', 5) == 10
        assert r.get('test:counter') == b'10'

        # Non-existent key
        assert r.incr('test:newcounter') == 1
        assert r.get('test:newcounter') == b'1'
        assert r.decr('test:newcounter2') == -1
        assert r.get('test:newcounter2') == b'-1'

        # Error cases
        r.set('test:notint', 'hello')
        with pytest.raises(Exception, match="value is not an integer"):
            r.incr('test:notint')

        with pytest.raises(Exception, match="value is not an integer"):
            r.decr('test:notint')

    def test_getset(self, redis_client: RedisClient):
        """Test GETSET command"""
        r = redis_client
        # Key exists
        r.set('test:getset:key1', 'oldvalue')
        old: bytes | None = r.getset('test:getset:key1', 'newvalue')
        assert old == b'oldvalue'
        assert r.get('test:getset:key1') == b'newvalue'

        # Key does not exist
        old = r.getset('test:getset:key2', 'firstvalue')
        assert old is None # Returns nil (None for client) when key didn't exist
        assert r.get('test:getset:key2') == b'firstvalue'

        # GETSET on non-string type
        r.lpush('test:getset:list', 'item')
        with pytest.raises(Exception, match="Operation against a key holding the wrong kind of value"):
            r.getset('test:getset:list', 'new')

    def test_mget(self, redis_client: RedisClient):
        """Test MGET command"""
        r = redis_client
        r.set('test:mget:key1', 'val1')
        r.set('test:mget:key2', 'val2')
        r.lpush('test:mget:list', 'item') # A non-string key

        results: list[bytes | None] = r.mget('test:mget:key1', 'test:mget:nonexistent', 'test:mget:key2', 'test:mget:list')
        expected: list[bytes | None] = [b'val1', None, b'val2', None] # Expect None for non-existent and wrong type
        assert results == expected

        # Empty list
        assert r.mget() == []

    def test_setnx(self, redis_client: RedisClient):
        """Test SETNX command"""
        r = redis_client
        # Key does not exist
        assert r.setnx('test:setnx:key1', 'value1') == 1
        assert r.get('test:setnx:key1') == b'value1'

        # Key exists
        assert r.setnx('test:setnx:key1', 'value2') == 0
        assert r.get('test:setnx:key1') == b'value1' # Value should not change

    def test_setex(self, redis_client: RedisClient):
        """Test SETEX command"""
        r = redis_client
        assert r.setex('test:setex:key1', 2, 'value') == 'OK'
        assert r.get('test:setex:key1') == b'value'
        ttl = r.ttl('test:setex:key1')
        assert 1 <= ttl <= 2, f"TTL ({ttl}) not within expected range [1, 2]"
        time.sleep(2.1)
        assert r.get('test:setex:key1') is None
        assert r.ttl('test:setex:key1') == -2

        # Invalid TTL (non-integer)
        with pytest.raises(Exception, match="value is not an integer"):
            # The client might raise TypeError or similar before sending
            # or the server might return an error.
            # Adjust match based on actual behavior.
            r.setex('test:setex:key2', 'notanumber', 'value') # type: ignore

        # Invalid TTL (negative)
        with pytest.raises(Exception, match="invalid expire time"):
            # Check specific error if server provides one, otherwise generic Exception
            r.setex('test:setex:key3', -10, 'value')

# No longer need nose specific execution block
# if __name__ == '__main__':
#    import nose
#    nose.runmodule()
