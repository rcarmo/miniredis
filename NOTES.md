# MiniRedis Project Notes

## Project Overview

MiniRedis is a simplified Redis server implementation in Python. It supports two server implementations:

1. A synchronous server (server.py)
2. An asynchronous server (aioserver.py) using asyncio

The project includes client implementation and test suites for both server types.

## Current State

As of May 6, 2025, the implementation has been fixed to address issues in the asynchronous server implementation.

### What Works

- Both synchronous and asynchronous server implementations now pass their respective test suites
- Redis protocol parsing and response formatting
- Connection handling in both server versions
- All basic Redis operations (GET/SET, KEYS, DELETE, etc.)
- Database selection and persistence across client operations
- Key expiration via TTL/EXPIRE/EXPIREAT
- Data persistence via the Haystack implementation
- Multi-database support

### Code Structure

- **server.py**: Synchronous Redis server implementation
- **aioserver.py**: Asynchronous Redis server implementation using asyncio
- **client.py**: Redis client implementation that works with both servers
- **haystack.py**: Simple key-value data persistence implementation
- **sset.py**: Sorted set implementation for Redis ZSET functionality
- **benchmark_client.py/benchmark_server.py**: Performance benchmarking tools

## Implementation Details

### Redis Protocol Support

The implementation handles Redis Serialization Protocol (RESP) for commands and responses:

- Bulk Strings for most data responses
- Simple Strings for status responses (e.g., "OK")
- Integers for numeric responses
- Error responses for invalid commands or errors
- Arrays for multi-part responses

### Key Commands Implemented

- DEL - Delete keys
- EXISTS - Check if keys exist
- EXPIRE/EXPIREAT - Set key timeout
- TTL/PTTL - Get key timeout
- KEYS - Find keys matching pattern
- TYPE - Get type of value stored at key

### String Commands Implemented

- GET - Get value of key
- SET - Set value of key
- APPEND - Append value to key
- INCR/INCRBY - Increment value of key
- DECR/DECRBY - Decrement value of key
- MGET - Get multiple keys
- GETSET - Set key and return previous value
- SETEX - Set key with expiration
- SETNX - Set key if it doesn't exist

### Other Data Types

- Lists (LPUSH, RPUSH, LPOP, RPOP, LRANGE, LLEN)
- Hashes (HSET, HGET, HGETALL, HDEL, HEXISTS, HINCRBY, HKEYS, HVALS, HLEN)
- Sorted Sets (ZADD, ZRANGE)
- PubSub functionality (PUBLISH, SUBSCRIBE, UNSUBSCRIBE)

### AsyncRedisServer Class Architecture

- Uses asyncio streams for network I/O
- Maintains connection contexts via task attributes
- Table-based multi-database support
- Background tasks for key expiration checking and auto-saving
- Command handlers follow a consistent pattern for error handling and Redis protocol response generation

### Data Persistence

- Uses Haystack class for simple key-value storage
- Data is persisted between server restarts
- Separate files for sync and async servers (redisdb.bin/idx vs redisdb_async.bin/idx)
- Auto-save functionality for data durability

## Test Suite Organization

### Test Files

- **test_keys.py**: Tests for key operations on sync server
- **test_keys_async.py**: Tests for key operations on async server
- **test_strings.py**: Tests for string operations on sync server
- **test_strings_async.py**: Tests for string operations on async server
- **helpers.py**: Helper functions for sync server tests
- **helpers_async.py**: Helper functions for async server tests

### Test Coverage

- Basic connectivity and protocol handling
- Key management operations
- String value operations
- Expiry functionality
- Multi-database operations
- Error handling and edge cases

## Running the Tests

To run the tests, use the following commands:

```bash
# Run all tests
python -m pytest

# Run only synchronous tests
python -m pytest tests/test_keys.py tests/test_strings.py

# Run only asynchronous tests
python -m pytest tests/test_keys_async.py tests/test_strings_async.py

# Run a specific test (with verbose output)
python -m pytest tests/test_keys_async.py::TestAsyncKeysCommands::test_put_get -v

# Run all async key tests with verbose output
python -m pytest tests/test_keys_async.py -v
```

### Test Fixtures

- Each test module has a fixture that starts the appropriate server
- For sync tests, the server runs in the same process
- For async tests, the server runs in a separate process via multiprocessing
- All tests use the same RedisClient implementation to interact with either server
- `redis_client_async` fixture in test_keys_async.py handles server startup/teardown

### Async Test Setup Details

The async test suite:

1. Finds a free port using `find_free_port()`
2. Starts the AsyncRedisServer in a separate process with `start_async_server()`
3. Connects to the server with the RedisClient
4. Flushes the database before each test
5. Runs the tests against the server
6. Tears down the server process with `stop_async_server()`

## Challenges Solved

1. **Connection Context Management**: Fixed issues with maintaining DB selection and connection state across client operations. The AsyncRedisServer now correctly associates each client with its selected database.

2. **Proper Task Cleanup**: Ensured all asyncio tasks are properly tracked and cleaned up when connections close or the server shuts down.

3. **Database Persistence**: Fixed how databases are stored and retrieved from the Haystack persistence layer.

4. **Key Expiration**: Implemented proper background task for checking key expiration and removing expired keys.

5. **Protocol Handling**: Enhanced the Redis protocol implementation to correctly handle all required data types and error conditions.

6. **TTL Management in SET Operations**: Fixed the SET command to properly remove any existing TTL when a key is updated with a new value, which aligns with standard Redis behavior.

7. **SETEX Response Format**: Corrected the SETEX command to return "OK" as a string rather than returning a boolean value, fixing client parsing errors.

## Recent Fixes (May 6, 2025)

### Fixed SET Command TTL Handling

The async implementation of the SET command was not removing existing TTL values when updating a key with a new value. This has been fixed to match Redis behavior where setting a key removes any existing expiration.

```python
# In handle_set method:
# Remove any expiration when setting a key (Redis behavior)
if isinstance(db_num, int) and f"{db_num} {key}" in self.timeouts:
    del self.timeouts[f"{db_num} {key}"]
```

### Fixed SETEX Response Type

The async implementation of SETEX was returning a boolean True value instead of the string "OK" as required by the Redis protocol. This was causing client-side parsing errors when handling responses.

```python
# In handle_setex method:
# Set the key and expiration
self.tables[db_num][key] = value
self.timeouts[f"{db_num} {key}"] = time.time() + ttl
return "OK"  # Return "OK" instead of True
```

These fixes ensure full compatibility with Redis clients and consistent behavior between the synchronous and asynchronous implementations.

## Future Improvements

- Add benchmarking results to compare sync vs async implementation performance
- Implement more Redis commands (sorting, bit operations, more advanced list/hash operations)
- Add more comprehensive logging for debugging and monitoring
- Consider connection pooling for more efficient client handling
- Implement transactions (MULTI/EXEC/WATCH)
- Add support for Redis modules
- Add more extensive error handling and recovery mechanisms
