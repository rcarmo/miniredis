#!/usr/bin/env python
# encoding: utf-8
"""
Asyncio-based Redis server implementation for miniredis.

Based on the original synchronous server.py.
"""

import asyncio
import logging
import time
import signal
import re
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union, Set

# Import Haystack and SortedSet
from .haystack import Haystack
from .sset import SortedSet

log = logging.getLogger()

# --- Simplified Error/Message Classes (similar to server.py) ---

class RedisError(Exception):
    def __init__(self, message: str):
        self.message = message
        super().__init__(message)

    def __str__(self) -> str:
        return f"-ERR {self.message}\r\n"

class RedisMessage:
    def __init__(self, message: str):
        self.message = message

    def __str__(self) -> str:
        return f"+{self.message}\r\n"

# --- Client Connection State ---
@dataclass
class AsyncRedisConnection:
    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter
    db: int = 0

# --- Redis Constants for Common Values ---
class RedisConstant:
    def __init__(self, type: str) -> None:
        self.type = type

    def __len__(self) -> int:
        return 0

    def __repr__(self) -> str:
        return f"<RedisConstant({self.type})>"

EMPTY_SCALAR = RedisConstant("EmptyScalar")
EMPTY_LIST = RedisConstant("EmptyList")
BAD_VALUE = RedisError("Operation against a key holding the wrong kind of value")

# --- Async Server Implementation ---

class AsyncRedisServer:
    def __init__(self, host: str = "127.0.0.1", port: int = 6379, db_path: str = ".") -> None:
        self.host = host
        self.port = port
        # Use tables for multi-db support
        self.tables: Dict[int, Dict[str, Any]] = {}
        self._server: Optional[asyncio.AbstractServer] = None
        self._tasks: set[asyncio.Task] = set()
        self.path = Path(db_path)
        self.meta = Haystack(self.path, "redisdb_async") # Use different filename
        # Expiry management
        self.timeouts: Dict[str, float] = {}
        # Keep track of last save
        self.lastsave = int(time.time())
        # Channels for PubSub
        self.channels: Dict[str, List[AsyncRedisConnection]] = {}
        # Track client connections by peername
        self.client_connections: Dict[str, AsyncRedisConnection] = {}
        # Load initial data
        self._load_data()
        log.info(f"AsyncRedisServer initialized for {host}:{port}, DB path: {self.path}")

    def _load_data(self) -> None:
        """Loads data from Haystack storage."""
        try:
            # Load timeouts
            self.timeouts = self.meta.get('timeouts', {})
            
            # Load tables for each DB found in meta
            db_keys = [k for k in self.meta.keys() if k.startswith('db_')]
            for db_key in db_keys:
                try:
                    db_num = int(db_key.split('_')[1])
                    self.tables[db_num] = self.meta.get(db_key, {})
                    log.info(f"Loaded data for DB {db_num}")
                except (ValueError, IndexError):
                    log.warning(f"Could not parse DB number from key: {db_key}")
            
            # Ensure DB 0 exists if no other DBs were loaded
            if 0 not in self.tables:
                self.tables[0] = {}
            
            log.info("Data loading complete.")
        except Exception as e:
            log.exception(f"Error loading data from {self.path}: {e}")
            # Ensure DB 0 exists even if loading fails
            if 0 not in self.tables:
                 self.tables[0] = {}

    async def save_data(self) -> None:
        """Saves current data to Haystack storage."""
        log.info("Saving data...")
        try:
            # Save timeouts
            self.meta['timeouts'] = self.timeouts
            
            # Save each DB table
            for db_num, table in self.tables.items():
                self.meta[f'db_{db_num}'] = table
                
            await asyncio.to_thread(self.meta.commit) # Run sync commit in thread
            self.lastsave = int(time.time())
            log.info("Data saved successfully.")
        except Exception as e:
            log.exception(f"Error saving data: {e}")

    async def check_ttl(self, db_num: int, key: str) -> bool:
        """Check if a key has expired. Returns True if key exists and is valid."""
        k = f"{db_num} {key}"
        if k in self.timeouts:
            if self.timeouts[k] <= time.time():
                # Key has expired - remove it
                if key in self.tables[db_num]:
                    del self.tables[db_num][key]
                    del self.timeouts[k]
                return False
        return key in self.tables[db_num]

    async def _encode_response(self, value: Any) -> bytes:
        """Encodes a Python value into the Redis protocol response."""
        if isinstance(value, bytes):
            return f"${len(value)}\r\n".encode() + value + b"\r\n"
        elif isinstance(value, str):
            # Special handling for "OK" responses - use simple string format
            if value == "OK":
                return b"+OK\r\n"
            encoded_value = value.encode()
            return f"${len(encoded_value)}\r\n".encode() + encoded_value + b"\r\n"
        elif isinstance(value, int):
            return f":{value}\r\n".encode()
        elif isinstance(value, RedisError):
            return str(value).encode()
        elif isinstance(value, RedisMessage):
             return str(value).encode()
        elif value is None:
            return b"$-1\r\n" # Null Bulk String
        elif isinstance(value, list):
             encoded_items = b"".join([await self._encode_response(item) for item in value])
             return f"*{len(value)}\r\n".encode() + encoded_items
        elif isinstance(value, bool): # Convert bool to integer (1=True, 0=False) for Redis protocol
             return f":{1 if value else 0}\r\n".encode()
        else:
            # Fallback for unknown types
            log.warning(f"Encoding unknown type: {type(value)}")
            return str(RedisError(f"Cannot encode type {type(value).__name__}")).encode()

    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        """Handles a single client connection."""
        peername = writer.get_extra_info('peername')
        connection = AsyncRedisConnection(reader=reader, writer=writer)
        connection_key = str(peername) if peername else f"anon-{id(connection)}"
        
        log.info(f"Client connected: {connection_key} (DB {connection.db})")
        task = asyncio.current_task()
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        
        # Store connection in the task for command handlers to access
        setattr(task, 'connection', connection)

        # Track the client connection with peername as key
        self.client_connections[connection_key] = connection
        log.info(f"Added client connection {connection_key} to tracking")

        # Ensure the client's selected DB exists
        if connection.db not in self.tables:
            self.tables[connection.db] = {}

        try:
            while True:
                try:
                    # 1. Read the command type and count
                    line = await reader.readline()
                    if not line or line == b'': # Connection closed
                        log.info(f"Client disconnected: {connection_key}")
                        break
                    if not line.startswith(b'*'):
                        writer.write(str(RedisError("Protocol error: expected array")).encode())
                        await writer.drain()
                        continue

                    try:
                        item_count = int(line[1:].strip())
                    except ValueError:
                        writer.write(str(RedisError("Protocol error: invalid array length")).encode())
                        await writer.drain()
                        continue

                    # 2. Read command arguments
                    args: List[bytes] = []
                    for _ in range(item_count):
                        # Read bulk string length
                        len_line = await reader.readline()
                        if not len_line or not len_line.startswith(b'$'):
                            raise RedisError("Protocol error: expected bulk string length")
                        try:
                            length = int(len_line[1:].strip())
                        except ValueError:
                             raise RedisError("Protocol error: invalid bulk string length")

                        if length == -1:
                            args.append(b"") # Represent null bulk string as empty bytes for simplicity here
                        else:
                            # Read bulk string data + CRLF
                            data = await reader.readexactly(length + 2)
                            if data[-2:] != b'\r\n':
                                raise RedisError("Protocol error: expected CRLF after bulk string")
                            args.append(data[:-2])

                    if not args:
                        continue # Should not happen if item_count > 0

                    # 3. Decode command and dispatch
                    command = args[0].decode().lower()
                    decoded_args = [arg.decode() for arg in args[1:]] # Decode remaining args

                    log.debug(f"Client {connection_key} executing command: {command} {decoded_args}")

                    handler_name = f"handle_{command}"
                    response: Any
                    if hasattr(self, handler_name):
                        handler = getattr(self, handler_name)
                        # Get current table based on connection's selected DB
                        if connection.db not in self.tables:
                            self.tables[connection.db] = {}
                        current_table = self.tables[connection.db]
                        
                        # Pass the current table to commands that operate directly on data
                        if command in ('set', 'get'):
                            response = await handler(current_table, *decoded_args)
                        elif command == 'select':
                            response = await handler(connection, *decoded_args)
                            # Update current_table if SELECT was successful
                            if not isinstance(response, RedisError):
                                if connection.db not in self.tables:
                                    self.tables[connection.db] = {}
                                log.info(f"Client {connection_key} switched to DB {connection.db}")
                        elif command in ('subscribe', 'unsubscribe', 'psubscribe', 'punsubscribe'):
                            await handler(connection, *decoded_args)
                            continue
                        else:
                            # For all other commands, temporarily ensure the task has the connection
                            # This creates a consistent context for all command handlers
                            old_connection = getattr(task, 'connection', None)
                            setattr(task, 'connection', connection)
                            try:
                                response = await handler(*decoded_args)
                            finally:
                                # Restore the original connection if there was one
                                if old_connection:
                                    setattr(task, 'connection', old_connection)
                                else:
                                    setattr(task, 'connection', connection)
                    else:
                        response = RedisError(f"unknown command '{command}'")

                    # 4. Send response
                    encoded_response = await self._encode_response(response)
                    writer.write(encoded_response)
                    await writer.drain()

                    # Special case for QUIT
                    if command == 'quit':
                        log.info(f"Client requested QUIT: {connection_key}")
                        break

                except (asyncio.IncompleteReadError, ConnectionResetError, BrokenPipeError) as e:
                    log.info(f"Connection error with {connection_key}: {e}")
                    break
                except RedisError as e:
                    log.warning(f"Redis error for {connection_key}: {e.message}")
                    writer.write(str(e).encode())
                    await writer.drain()
                except Exception as e:
                    log.exception(f"Unexpected error handling client {connection_key}: {e}")
                    try:
                        writer.write(str(RedisError(f"Internal server error: {e}")).encode())
                        await writer.drain()
                    except (ConnectionResetError, BrokenPipeError):
                        pass # Client likely disconnected
                    break # Stop handling this client on unexpected errors
        finally:
            # Cleanup
            try:
                writer.close()
                await writer.wait_closed()
            except Exception as e:
                log.debug(f"Error during writer close for {connection_key}: {e}")
                
            log.info(f"Connection closed for {connection_key}")
            # Remove connection from task when done
            if hasattr(task, 'connection'):
                delattr(task, 'connection')
                
            # Remove the client connection from tracking
            if connection_key in self.client_connections:
                log.info(f"Removing client connection {connection_key} from tracking")
                del self.client_connections[connection_key]

    # --- Command Handlers ---

    async def handle_ping(self, *args: str) -> RedisMessage:
        log.debug("Handling PING")
        if len(args) == 1:
            return RedisMessage(args[0]) # Echo message
        elif len(args) == 0:
            return RedisMessage("PONG")
        else:
            return RedisError("wrong number of arguments for 'ping' command")

    async def handle_set(self, table: Dict[str, Any], key: str, value: str, *options: str) -> str:
        # Find the DB number safely for logging
        db_nums = [db for db, t in self.tables.items() if t is table]
        db_num = db_nums[0] if db_nums else "unknown"
        log.debug(f"Handling SET {key} = {value} in DB {db_num}")
        
        if not key or value is None: # Basic validation
             return RedisError("wrong number of arguments for 'set' command")
             
        # Remove any expiration when setting a key (Redis behavior)
        if isinstance(db_num, int) and f"{db_num} {key}" in self.timeouts:
            del self.timeouts[f"{db_num} {key}"]
            
        table[key] = value # Use the passed table
        return "OK" # Returns "+OK" in Redis protocol

    async def handle_get(self, table: Dict[str, Any], key: str) -> Optional[str]:
        # Find the DB number safely for logging
        db_nums = [db for db, t in self.tables.items() if t is table]
        db_num = db_nums[0] if db_nums else "unknown"
        log.debug(f"Handling GET {key} in DB {db_num}")
        
        if not key:
            return RedisError("wrong number of arguments for 'get' command")
        return table.get(key) # Use the passed table

    async def handle_select(self, connection: AsyncRedisConnection, db_index_str: str) -> bool:
        """Select the DB for the current connection."""
        try:
            db_index = int(db_index_str)
            if db_index < 0:
                 raise ValueError("DB index must be positive")
        except ValueError:
            return RedisError("invalid DB index")

        # Ensure the target DB exists in self.tables
        if db_index not in self.tables:
            self.tables[db_index] = {}

        connection.db = db_index
        # The current_table in handle_client will be updated after this returns
        return True # Returns +OK

    async def handle_save(self) -> bool:
         """Explicitly trigger data saving."""
         await self.save_data()
         return True # Returns +OK

    async def handle_quit(self) -> RedisMessage:
        # Response is sent before connection is closed by the handler loop
        return RedisMessage("OK")

    # --- Server Management Commands ---
    
    async def handle_flushdb(self) -> int:
        """Remove all keys from the current DB."""
        # Try all DBs if we don't have connection context
        if 'connection' in locals():
            db_nums = [connection.db]
        else:
            db_nums = list(self.tables.keys())
            
        for db_num in db_nums:
            # Clear the DB
            self.tables[db_num] = {}
            
            # Remove any timeouts for this DB
            for timeout_key in list(self.timeouts.keys()):
                if timeout_key.startswith(f"{db_num} "):
                    del self.timeouts[timeout_key]
        
        return 1  # Return integer 1 for Redis protocol OK
        
    async def handle_flushall(self) -> int:
        """Remove all keys from all DBs."""
        # Clear all DBs
        for db_num in list(self.tables.keys()):
            self.tables[db_num] = {}
            
        # Clear all timeouts
        self.timeouts = {}
            
        return 1  # Return integer 1 for Redis protocol OK

    # --- PubSub Commands ---

    async def handle_publish(self, channel: str, message: str) -> int:
        """Publish a message to a channel"""
        if not channel or message is None:
            return RedisError("wrong number of arguments for 'publish' command")

        published_count = 0
        
        # Check for exact channel matches
        if channel in self.channels:
            for connection in self.channels[channel]:
                try:
                    # Format message in Redis protocol
                    # *3\r\n$7\r\nmessage\r\n${len(channel)}\r\n{channel}\r\n${len(message)}\r\n{message}\r\n
                    msg = [
                        "message",
                        channel,
                        message
                    ]
                    encoded = await self._encode_response(msg)
                    connection.writer.write(encoded)
                    await connection.writer.drain()
                    published_count += 1
                except (ConnectionError, BrokenPipeError, asyncio.CancelledError) as e:
                    log.warning(f"Error publishing to client: {e}")
                    # Will remove broken connections during next subscription
        
        # Check for pattern matches
        for pattern, connections in self.channels.items():
            # Skip exact channels we already processed
            if pattern == channel:
                continue
                
            # Check if pattern matches
            try:
                if '*' in pattern or '?' in pattern:
                    # Convert Redis glob pattern to regex
                    regex_pattern = pattern.replace('*', '.*').replace('?', '.')
                    if re.match(f"^{regex_pattern}$", channel):
                        for connection in connections:
                            try:
                                # Format pmessage in Redis protocol
                                msg = [
                                    "pmessage",
                                    pattern,
                                    channel,
                                    message
                                ]
                                encoded = await self._encode_response(msg)
                                connection.writer.write(encoded)
                                await connection.writer.drain()
                                published_count += 1
                            except (ConnectionError, BrokenPipeError, asyncio.CancelledError) as e:
                                log.warning(f"Error publishing to pattern subscriber: {e}")
            except re.error:
                # Skip invalid patterns
                continue
                
        return published_count

    async def handle_subscribe(self, connection: AsyncRedisConnection, *channels: str) -> None:
        """Subscribe to channels"""
        if not channels:
            return RedisError("wrong number of arguments for 'subscribe' command")
        
        # Subscription count
        count = 0
        
        # For each channel
        for channel in channels:
            # Create channel list if it doesn't exist
            if channel not in self.channels:
                self.channels[channel] = []
                
            # Add connection to channel subscribers if not already there
            if connection not in self.channels[channel]:
                self.channels[channel].append(connection)
                
            # Send subscription confirmation to client
            # Format: *3\r\n$9\r\nsubscribe\r\n${len(channel)}\r\n{channel}\r\n:{count}\r\n
            try:
                count += 1
                msg = [
                    "subscribe",
                    channel,
                    count
                ]
                encoded = await self._encode_response(msg)
                connection.writer.write(encoded)
                await connection.writer.drain()
            except (ConnectionError, BrokenPipeError, asyncio.CancelledError) as e:
                log.warning(f"Error sending subscribe confirmation: {e}")
                
        # Note: For true Redis behavior, connections in subscription mode
        # should only accept subscription-related commands until unsubscribed.
        # This would require modifying the handle_client method.
        return None

    async def handle_unsubscribe(self, connection: AsyncRedisConnection, *channels: str) -> None:
        """Unsubscribe from channels"""
        # If no channels specified, unsubscribe from all
        if not channels:
            channels_to_check = list(self.channels.keys())
        else:
            channels_to_check = channels
            
        count = 0
        
        for channel in channels_to_check:
            if channel in self.channels and connection in self.channels[channel]:
                # Remove connection from channel subscribers
                self.channels[channel].remove(connection)
                
                # If channel has no subscribers, remove it
                if not self.channels[channel]:
                    del self.channels[channel]
                    
                # Send unsubscription confirmation to client
                try:
                    count += 1
                    msg = [
                        "unsubscribe",
                        channel,
                        count
                    ]
                    encoded = await self._encode_response(msg)
                    connection.writer.write(encoded)
                    await connection.writer.drain()
                except (ConnectionError, BrokenPipeError, asyncio.CancelledError) as e:
                    log.warning(f"Error sending unsubscribe confirmation: {e}")
                    
        return None

    async def handle_psubscribe(self, connection: AsyncRedisConnection, *patterns: str) -> None:
        """Subscribe to channels matching patterns"""
        if not patterns:
            return RedisError("wrong number of arguments for 'psubscribe' command")
            
        count = 0
        
        for pattern in patterns:
            # Create pattern list if it doesn't exist
            if pattern not in self.channels:
                self.channels[pattern] = []
                
            # Add connection to pattern subscribers if not already there
            if connection not in self.channels[pattern]:
                self.channels[pattern].append(connection)
                
            # Send subscription confirmation to client
            try:
                count += 1
                msg = [
                    "psubscribe",
                    pattern,
                    count
                ]
                encoded = await self._encode_response(msg)
                connection.writer.write(encoded)
                await connection.writer.drain()
            except (ConnectionError, BrokenPipeError, asyncio.CancelledError) as e:
                log.warning(f"Error sending psubscribe confirmation: {e}")
                
        return None

    async def handle_punsubscribe(self, connection: AsyncRedisConnection, *patterns: str) -> None:
        """Unsubscribe from channels matching patterns"""
        if not patterns:
            # Unsubscribe from all patterns
            # In a real implementation, we would need to differentiate between patterns and channels
            # For this simple implementation, we assume patterns contain * or ?
            patterns_to_check = [p for p in self.channels.keys() if '*' in p or '?' in p]
        else:
            patterns_to_check = patterns
            
        count = 0
        
        for pattern in patterns_to_check:
            if pattern in self.channels and connection in self.channels[pattern]:
                # Remove connection from pattern subscribers
                self.channels[pattern].remove(connection)
                
                # If pattern has no subscribers, remove it
                if not self.channels[pattern]:
                    del self.channels[pattern]
                    
                # Send unsubscription confirmation to client
                try:
                    count += 1
                    msg = [
                        "punsubscribe",
                        pattern,
                        count
                    ]
                    encoded = await self._encode_response(msg)
                    connection.writer.write(encoded)
                    await connection.writer.drain()
                except (ConnectionError, BrokenPipeError, asyncio.CancelledError) as e:
                    log.warning(f"Error sending punsubscribe confirmation: {e}")
                    
        return None

    # --- Redis Key Commands ---

    async def handle_del(self, *args: str) -> int:
        """Delete one or more keys, returns the number of keys removed"""
        if not args:
            return RedisError("wrong number of arguments for 'del' command")
        
        # Get the correct DB from the current task's connection
        task = asyncio.current_task()
        if task and hasattr(task, 'connection'):
            connection = getattr(task, 'connection')
            db_num = connection.db
        else:
            db_num = 0  # Default to DB 0
        
        count = 0
        for key in args:
            # Check if key exists and remove timeouts
            timeout_key = f"{db_num} {key}"
            if timeout_key in self.timeouts:
                del self.timeouts[timeout_key]
            
            # Delete the key from DB
            if key in self.tables[db_num]:
                del self.tables[db_num][key]
                count += 1
        
        return count

    async def handle_exists(self, *keys: str) -> int:
        """Check if one or more keys exist"""
        if not keys:
            return RedisError("wrong number of arguments for 'exists' command")
        
        # Get the correct DB from the current task's connection
        task = asyncio.current_task()
        if task and hasattr(task, 'connection'):
            connection = getattr(task, 'connection')
            db_num = connection.db
        else:
            db_num = 0  # Default to DB 0 if no connection context
            
        count = 0
        for key in keys:
            # Check if key exists in the current DB
            if key in self.tables[db_num]:
                # Check TTL - skip if expired
                if await self.check_ttl(db_num, key):
                    count += 1
                    
        return count

    async def handle_expire(self, key: str, seconds: str) -> int:
        """Set a key's time to live in seconds"""
        if not key or not seconds:
            return RedisError("wrong number of arguments for 'expire' command")
        
        try:
            ttl = int(seconds)
        except ValueError:
            return RedisError("value is not an integer or out of range")
        
        # Get the correct DB from the current task's connection
        task = asyncio.current_task()
        if task and hasattr(task, 'connection'):
            connection = getattr(task, 'connection')
            db_num = connection.db
            
            # Check if key exists in this specific db
            if key in self.tables[db_num]:
                self.timeouts[f"{db_num} {key}"] = time.time() + ttl
                return 1
            return 0  # Key doesn't exist
        else:
            # Fallback to checking all DBs
            db_nums = list(self.tables.keys())
            for db_num in db_nums:
                if key in self.tables[db_num]:
                    self.timeouts[f"{db_num} {key}"] = time.time() + ttl
                    return 1
            return 0

    async def handle_expireat(self, key: str, timestamp: str) -> int:
        """Set the expiration for a key at a UNIX timestamp"""
        if not key or not timestamp:
            return RedisError("wrong number of arguments for 'expireat' command")
        
        try:
            ts = int(timestamp)
        except ValueError:
            return RedisError("value is not an integer or out of range")
        
        # Get the correct DB from the current task's connection
        task = asyncio.current_task()
        if task and hasattr(task, 'connection'):
            connection = getattr(task, 'connection')
            db_nums = [connection.db]
        else:
            db_nums = list(self.tables.keys())
        
        for db_num in db_nums:
            if key in self.tables[db_num]:
                self.timeouts[f"{db_num} {key}"] = ts
                return 1
        
        return 0  # Key does not exist

    async def handle_ttl(self, key: str) -> int:
        """Get the time to live for a key in seconds"""
        if not key:
            return RedisError("wrong number of arguments for 'ttl' command")
            
        # Get the correct DB from the current task's connection
        task = asyncio.current_task()
        if task and hasattr(task, 'connection'):
            connection = getattr(task, 'connection')
            db_nums = [connection.db]
        else:
            db_nums = list(self.tables.keys())
        
        for db_num in db_nums:
            # Check if the key exists
            if not await self.check_ttl(db_num, key):
                continue
                
            if key not in self.tables[db_num]:
                continue
                
            timeout_key = f"{db_num} {key}"
            if timeout_key in self.timeouts:
                ttl = int(self.timeouts[timeout_key] - time.time())
                return max(0, ttl)  # Return at least 0
            else:
                return -1  # Key exists but has no TTL
                
        return -2  # Key does not exist

    async def handle_pttl(self, key: str) -> int:
        """Get the time to live for a key in milliseconds"""
        ttl = await self.handle_ttl(key)
        if isinstance(ttl, RedisError) or ttl < 0:
            return ttl  # Pass through error or special values
        return ttl * 1000  # Convert to milliseconds

    async def handle_pexpire(self, key: str, milliseconds: str) -> int:
        """Set a key's time to live in milliseconds"""
        if not key or not milliseconds:
            return RedisError("wrong number of arguments for 'pexpire' command")
        
        try:
            ms = int(milliseconds)
            seconds = ms / 1000
        except ValueError:
            return RedisError("value is not an integer or out of range")
            
        return await self.handle_expire(key, str(seconds))

    async def handle_persist(self, key: str) -> int:
        """Remove the expiration from a key"""
        if not key:
            return RedisError("wrong number of arguments for 'persist' command")
            
        # Try all DBs if we don't have connection context
        if 'connection' in locals():
            db_nums = [connection.db]
        else:
            db_nums = list(self.tables.keys())
        
        for db_num in db_nums:
            if key in self.tables[db_num]:
                timeout_key = f"{db_num} {key}"
                if timeout_key in self.timeouts:
                    del self.timeouts[timeout_key]
                    return 1
        
        return 0

    async def handle_keys(self, pattern: str) -> List[bytes]:
        """Find all keys matching the given pattern"""
        if not pattern:
            return RedisError("wrong number of arguments for 'keys' command")
            
        # Get the correct DB from the current task's connection
        task = asyncio.current_task()
        if task and hasattr(task, 'connection'):
            connection = getattr(task, 'connection')
            db_num = connection.db
        else:
            db_num = 0  # Default to DB 0 if no connection context
            
        matching_keys = []
        
        # Properly escape regex special characters in the pattern except * and ?
        pattern_regex = re.escape(pattern).replace('\\*', '.*').replace('\\?', '.')
        regex = re.compile(f"^{pattern_regex}$")
        
        # Look through each key in the specific DB only
        if db_num in self.tables:
            for key in list(self.tables[db_num].keys()):
                # Skip expired keys
                if not await self.check_ttl(db_num, key):
                    continue
                    
                # Check if key matches pattern
                if regex.match(key):
                    # Return key as bytes for Redis protocol compatibility
                    matching_keys.append(key.encode())
                    
        return matching_keys

    async def handle_type(self, key: str) -> RedisMessage:
        """Determine the type stored at key"""
        if not key:
            return RedisError("wrong number of arguments for 'type' command")
            
        # Try all DBs if we don't have connection context
        if 'connection' in locals():
            db_nums = [connection.db]
        else:
            db_nums = list(self.tables.keys())
        
        for db_num in db_nums:
            if not await self.check_ttl(db_num, key):
                continue
                
            if key not in self.tables[db_num]:
                continue
                
            data = self.tables[db_num][key]
            
            if isinstance(data, str):
                return RedisMessage("string")
            elif isinstance(data, dict):
                return RedisMessage("hash")
            elif isinstance(data, deque):
                return RedisMessage("list")
            elif isinstance(data, set):
                return RedisMessage("set")
            elif isinstance(data, SortedSet):
                return RedisMessage("zset")
            else:
                return RedisMessage("unknown")
                
        return RedisMessage("none")

    # --- String Commands ---
    
    async def handle_append(self, key: str, value: str) -> int:
        """Append a value to a key"""
        if not key or not value:
            return RedisError("wrong number of arguments for 'append' command")
            
        # Try all DBs if we don't have connection context
        if 'connection' in locals():
            db_nums = [connection.db]
        else:
            db_nums = list(self.tables.keys())
            
        for db_num in db_nums:
            # Check if the key exists
            if await self.check_ttl(db_num, key):
                if key in self.tables[db_num]:
                    data = self.tables[db_num][key]
                    # Check if it's a string type
                    if isinstance(data, str):
                        self.tables[db_num][key] = data + value
                        return len(self.tables[db_num][key])
                    else:
                        return BAD_VALUE
                        
            # If key doesn't exist, create it
            self.tables[db_num][key] = value
            return len(value)
        
        # Should not reach here if at least one DB exists
        return 0
        
    async def handle_incr(self, key: str) -> int:
        """Increment the integer value of a key by one"""
        return await self.handle_incrby(key, "1")
        
    async def handle_decr(self, key: str) -> int:
        """Decrement the integer value of a key by one"""
        return await self.handle_incrby(key, "-1")
        
    async def handle_incrby(self, key: str, increment: str) -> int:
        """Increment the integer value of a key by the given amount"""
        if not key or not increment:
            return RedisError("wrong number of arguments for 'incrby' command")
            
        try:
            incr = int(increment)
        except ValueError:
            return RedisError("value is not an integer or out of range")
            
        # Try all DBs if we don't have connection context
        if 'connection' in locals():
            db_nums = [connection.db]
        else:
            db_nums = list(self.tables.keys())
            
        for db_num in db_nums:
            # Check if key exists and is valid
            if await self.check_ttl(db_num, key):
                if key in self.tables[db_num]:
                    current_value = self.tables[db_num][key]
                    try:
                        # Try to convert current value to int
                        current_int = int(current_value)
                        new_value = current_int + incr
                        self.tables[db_num][key] = str(new_value)
                        return new_value
                    except (ValueError, TypeError):
                        return RedisError("value is not an integer or out of range")
            
            # Key doesn't exist - create with value of increment
            self.tables[db_num][key] = str(incr)
            return incr
            
        # Should not reach here if at least one DB exists
        return 0
        
    async def handle_decrby(self, key: str, decrement: str) -> int:
        """Decrement the integer value of a key by the given amount"""
        try:
            decr = int(decrement)
        except ValueError:
            return RedisError("value is not an integer or out of range")
        # Use incrby with negated value    
        return await self.handle_incrby(key, str(-decr))
        
    async def handle_mget(self, *keys: str) -> List[Optional[str]]:
        """Get the values of all specified keys"""
        result = []
        
        # Try all DBs if we don't have connection context
        if 'connection' in locals():
            db_nums = [connection.db]
        else:
            db_nums = list(self.tables.keys())
            
        for key in keys:
            found = False
            for db_num in db_nums:
                if await self.check_ttl(db_num, key):
                    if key in self.tables[db_num]:
                        data = self.tables[db_num][key]
                        if isinstance(data, str):
                            result.append(data)
                            found = True
                            break
                        else:
                            # Wrong type
                            result.append(None)
                            found = True
                            break
            if not found:
                result.append(None)
                
        return result
        
    async def handle_getset(self, key: str, value: str) -> Optional[str]:
        """Set the string value of a key and return its old value"""
        if not key or value is None:
            return RedisError("wrong number of arguments for 'getset' command")
            
        # Try all DBs if we don't have connection context
        if 'connection' in locals():
            db_nums = [connection.db]
        else:
            db_nums = list(self.tables.keys())
            
        for db_num in db_nums:
            old_value = None
            if await self.check_ttl(db_num, key):
                if key in self.tables[db_num]:
                    old_value = self.tables[db_num][key]
                    if not isinstance(old_value, str):
                        return BAD_VALUE
            
            # Set the new value
            self.tables[db_num][key] = value
            return old_value
            
        # Should not reach here if at least one DB exists
        return None
        
    async def handle_setex(self, key: str, seconds: str, value: str) -> str:
        """Set the value and expiration of a key"""
        if not key or not seconds or value is None:
            return RedisError("wrong number of arguments for 'setex' command")
            
        try:
            ttl = int(seconds)
            if ttl <= 0:
                return RedisError("invalid expire time in 'setex' command")
        except ValueError:
            return RedisError("value is not an integer or out of range")
            
        # Try all DBs if we don't have connection context
        task = asyncio.current_task()
        if task and hasattr(task, 'connection'):
            connection = getattr(task, 'connection')
            db_nums = [connection.db]
        else:
            db_nums = list(self.tables.keys())
            
        for db_num in db_nums:
            # Set the key
            self.tables[db_num][key] = value
            # Set expiration
            self.timeouts[f"{db_num} {key}"] = time.time() + ttl
            return "OK"  # Return "OK" instead of True
            
        # Should not reach here if at least one DB exists
        return "OK"  # Default to OK
        
    async def handle_setnx(self, key: str, value: str) -> int:
        """Set the value of a key, only if the key does not exist"""
        if not key or value is None:
            return RedisError("wrong number of arguments for 'setnx' command")
            
        # Try all DBs if we don't have connection context
        if 'connection' in locals():
            db_nums = [connection.db]
        else:
            db_nums = list(self.tables.keys())
            
        for db_num in db_nums:
            # Check if key exists
            if await self.check_ttl(db_num, key) and key in self.tables[db_num]:
                return 0  # Key exists, do nothing
                
            # Set key if it doesn't exist
            self.tables[db_num][key] = value
            return 1
            
        # Should not reach here if at least one DB exists
        return 0

    # --- List Commands ---

    async def handle_lpush(self, key: str, value: str, *values: str) -> int:
        """Push one or more values to the head of a list"""
        if not key or value is None:
            return RedisError("wrong number of arguments for 'lpush' command")
            
        # Try all DBs if we don't have connection context
        if 'connection' in locals():
            db_nums = [connection.db]
        else:
            db_nums = list(self.tables.keys())
            
        for db_num in db_nums:
            # Check if key exists
            if await self.check_ttl(db_num, key) and key in self.tables[db_num]:
                # Get the existing list
                data = self.tables[db_num][key]
                if not isinstance(data, deque):
                    return BAD_VALUE
                    
                # Add new values
                data.appendleft(value)
                for v in values:
                    data.appendleft(v)
                return len(data)
                
            # Create a new list
            all_values = [value] + list(values)
            q = deque(all_values)
            self.tables[db_num][key] = q
            return len(q)
            
        # Should not reach here if at least one DB exists
        return 0
    
    async def handle_rpush(self, key: str, value: str, *values: str) -> int:
        """Push one or more values to the tail of a list"""
        if not key or value is None:
            return RedisError("wrong number of arguments for 'rpush' command")
            
        # Try all DBs if we don't have connection context
        if 'connection' in locals():
            db_nums = [connection.db]
        else:
            db_nums = list(self.tables.keys())
            
        for db_num in db_nums:
            # Check if key exists
            if await self.check_ttl(db_num, key) and key in self.tables[db_num]:
                # Get the existing list
                data = self.tables[db_num][key]
                if not isinstance(data, deque):
                    return BAD_VALUE
                    
                # Add new values
                data.append(value)
                for v in values:
                    data.append(v)
                return len(data)
                
            # Create a new list
            all_values = [value] + list(values)
            q = deque(all_values)
            self.tables[db_num][key] = q
            return len(q)
            
        # Should not reach here if at least one DB exists
        return 0
    
    async def handle_lpop(self, key: str) -> str:
        """Remove and return the first element of a list"""
        if not key:
            return RedisError("wrong number of arguments for 'lpop' command")
            
        # Try all DBs if we don't have connection context
        if 'connection' in locals():
            db_nums = [connection.db]
        else:
            db_nums = list(self.tables.keys())
            
        for db_num in db_nums:
            # Check if key exists
            if not await self.check_ttl(db_num, key) or key not in self.tables[db_num]:
                continue
                
            data = self.tables[db_num][key]
            if not isinstance(data, deque):
                return BAD_VALUE
                
            if len(data) == 0:
                return None
                
            return data.popleft()
            
        return None  # Key does not exist
    
    async def handle_rpop(self, key: str) -> str:
        """Remove and return the last element of a list"""
        if not key:
            return RedisError("wrong number of arguments for 'rpop' command")
            
        # Try all DBs if we don't have connection context
        if 'connection' in locals():
            db_nums = [connection.db]
        else:
            db_nums = list(self.tables.keys())
            
        for db_num in db_nums:
            # Check if key exists
            if not await self.check_ttl(db_num, key) or key not in self.tables[db_num]:
                continue
                
            data = self.tables[db_num][key]
            if not isinstance(data, deque):
                return BAD_VALUE
                
            if len(data) == 0:
                return None
                
            return data.pop()
            
        return None  # Key does not exist
    
    async def handle_llen(self, key: str) -> int:
        """Get the length of a list"""
        if not key:
            return RedisError("wrong number of arguments for 'llen' command")
            
        # Try all DBs if we don't have connection context
        if 'connection' in locals():
            db_nums = [connection.db]
        else:
            db_nums = list(self.tables.keys())
            
        for db_num in db_nums:
            # Check if key exists
            if not await self.check_ttl(db_num, key) or key not in self.tables[db_num]:
                continue
                
            data = self.tables[db_num][key]
            if not isinstance(data, deque):
                return BAD_VALUE
                
            return len(data)
            
        return 0  # Key does not exist
    
    async def handle_lrange(self, key: str, start: str, stop: str) -> list:
        """Get a range of elements from a list"""
        if not key or start is None or stop is None:
            return RedisError("wrong number of arguments for 'lrange' command")
            
        try:
            start_idx = int(start)
            stop_idx = int(stop)
        except ValueError:
            return RedisError("value is not an integer or out of range")
            
        # Try all DBs if we don't have connection context
        if 'connection' in locals():
            db_nums = [connection.db]
        else:
            db_nums = list(self.tables.keys())
            
        for db_num in db_nums:
            # Check if key exists
            if not await self.check_ttl(db_num, key) or key not in self.tables[db_num]:
                continue
                
            data = self.tables[db_num][key]
            if not isinstance(data, deque):
                return BAD_VALUE
                
            # Convert to list for easier slicing
            l = list(data)
            
            # Adjust negative indices
            if start_idx < 0:
                start_idx = len(l) + start_idx
            if stop_idx < 0:
                stop_idx = len(l) + stop_idx
                
            # Clamp indices
            start_idx = max(0, start_idx)
            stop_idx = min(len(l) - 1, stop_idx)
            
            # Return the range (inclusive on both ends like Redis)
            return l[start_idx:stop_idx + 1] if start_idx <= stop_idx else []
            
        return []  # Key does not exist

    # --- Hash Commands ---

    async def handle_hset(self, key: str, field: str, value: str) -> int:
        """Set field in the hash stored at key to value"""
        if not key or not field or value is None:
            return RedisError("wrong number of arguments for 'hset' command")
            
        # Try all DBs if we don't have connection context
        if 'connection' in locals():
            db_nums = [connection.db]
        else:
            db_nums = list(self.tables.keys())
            
        for db_num in db_nums:
            # Check if key exists
            if await self.check_ttl(db_num, key) and key in self.tables[db_num]:
                data = self.tables[db_num][key]
                if not isinstance(data, dict):
                    return BAD_VALUE
                    
                is_new = field not in data
                data[field] = value
                return 1 if is_new else 0
                
            # Create a new hash
            self.tables[db_num][key] = {field: value}
            return 1
            
        # Should not reach here if at least one DB exists
        return 0
    
    async def handle_hget(self, key: str, field: str) -> str:
        """Get the value of a hash field"""
        if not key or not field:
            return RedisError("wrong number of arguments for 'hget' command")
            
        # Try all DBs if we don't have connection context
        if 'connection' in locals():
            db_nums = [connection.db]
        else:
            db_nums = list(self.tables.keys())
            
        for db_num in db_nums:
            # Check if key exists
            if not await self.check_ttl(db_num, key) or key not in self.tables[db_num]:
                continue
                
            data = self.tables[db_num][key]
            if not isinstance(data, dict):
                return BAD_VALUE
                
            if field not in data:
                return None
                
            return data[field]
            
        return None  # Key does not exist
    
    async def handle_hgetall(self, key: str) -> Union[list, RedisError]:
        """Get all fields and values in a hash"""
        if not key:
            return RedisError("wrong number of arguments for 'hgetall' command")
            
        # Try all DBs if we don't have connection context
        if 'connection' in locals():
            db_nums = [connection.db]
        else:
            db_nums = list(self.tables.keys())
            
        for db_num in db_nums:
            # Check if key exists
            if not await self.check_ttl(db_num, key) or key not in self.tables[db_num]:
                continue
                
            data = self.tables[db_num][key]
            if not isinstance(data, dict):
                return BAD_VALUE
                
            # Convert dict to flat list of [key, value, key, value, ...]
            result = []
            for k, v in data.items():
                result.append(k)
                result.append(v)
            return result
            
        return []  # Key does not exist or is empty
    
    async def handle_hdel(self, key: str, *fields: str) -> int:
        """Delete one or more hash fields"""
        if not key or not fields:
            return RedisError("wrong number of arguments for 'hdel' command")
            
        # Try all DBs if we don't have connection context
        if 'connection' in locals():
            db_nums = [connection.db]
        else:
            db_nums = list(self.tables.keys())
            
        for db_num in db_nums:
            # Check if key exists
            if not await self.check_ttl(db_num, key) or key not in self.tables[db_num]:
                continue
                
            data = self.tables[db_num][key]
            if not isinstance(data, dict):
                return BAD_VALUE
                
            removed = 0
            for field in fields:
                if field in data:
                    del data[field]
                    removed += 1
            
            return removed
            
        return 0  # Key does not exist
    
    async def handle_hexists(self, key: str, field: str) -> int:
        """Determine if a hash field exists"""
        if not key or not field:
            return RedisError("wrong number of arguments for 'hexists' command")
            
        # Try all DBs if we don't have connection context
        if 'connection' in locals():
            db_nums = [connection.db]
        else:
            db_nums = list(self.tables.keys())
            
        for db_num in db_nums:
            # Check if key exists
            if not await self.check_ttl(db_num, key) or key not in self.tables[db_num]:
                continue
                
            data = self.tables[db_num][key]
            if not isinstance(data, dict):
                return BAD_VALUE
                
            return 1 if field in data else 0
            
        return 0  # Key does not exist
    
    async def handle_hincrby(self, key: str, field: str, increment: str) -> int:
        """Increment the integer value of a hash field"""
        if not key or not field or not increment:
            return RedisError("wrong number of arguments for 'hincrby' command")
            
        try:
            incr = int(increment)
        except ValueError:
            return RedisError("value is not an integer or out of range")
            
        # Try all DBs if we don't have connection context
        if 'connection' in locals():
            db_nums = [connection.db]
        else:
            db_nums = list(self.tables.keys())
            
        for db_num in db_nums:
            # Check if key exists or create a new hash
            if not await self.check_ttl(db_num, key) or key not in self.tables[db_num]:
                self.tables[db_num][key] = {}
                
            data = self.tables[db_num][key]
            if not isinstance(data, dict):
                return BAD_VALUE
                
            try:
                # Get current value or default to 0
                current = data.get(field, "0")
                current_int = int(current)
                new_value = current_int + incr
                # Store result as string to match Redis behavior
                data[field] = str(new_value)
                return new_value
            except ValueError:
                return RedisError("hash value is not an integer")
                
        # Should not reach here if at least one DB exists
        return 0
    
    async def handle_hkeys(self, key: str) -> list:
        """Get all the fields in a hash"""
        if not key:
            return RedisError("wrong number of arguments for 'hkeys' command")
            
        # Try all DBs if we don't have connection context
        if 'connection' in locals():
            db_nums = [connection.db]
        else:
            db_nums = list(self.tables.keys())
            
        for db_num in db_nums:
            # Check if key exists
            if not await self.check_ttl(db_num, key) or key not in self.tables[db_num]:
                continue
                
            data = self.tables[db_num][key]
            if not isinstance(data, dict):
                return BAD_VALUE
                
            return list(data.keys())
            
        return []  # Key does not exist or is empty
    
    async def handle_hvals(self, key: str) -> list:
        """Get all the values in a hash"""
        if not key:
            return RedisError("wrong number of arguments for 'hvals' command")
            
        # Try all DBs if we don't have connection context
        if 'connection' in locals():
            db_nums = [connection.db]
        else:
            db_nums = list(self.tables.keys())
            
        for db_num in db_nums:
            # Check if key exists
            if not await self.check_ttl(db_num, key) or key not in self.tables[db_num]:
                continue
                
            data = self.tables[db_num][key]
            if not isinstance(data, dict):
                return BAD_VALUE
                
            return list(data.values())
            
        return []  # Key does not exist or is empty
    
    async def handle_hlen(self, key: str) -> int:
        """Get the number of fields in a hash"""
        if not key:
            return RedisError("wrong number of arguments for 'hlen' command")
            
        # Try all DBs if we don't have connection context
        if 'connection' in locals():
            db_nums = [connection.db]
        else:
            db_nums = list(self.tables.keys())
            
        for db_num in db_nums:
            # Check if key exists
            if not await self.check_ttl(db_num, key) or key not in self.tables[db_num]:
                continue
                
            data = self.tables[db_num][key]
            if not isinstance(data, dict):
                return BAD_VALUE
                
            return len(data)
            
        return 0  # Key does not exist

    # --- Sorted Set Commands ---
    
    async def handle_zadd(self, key: str, score: str, member: str, *args: str) -> int:
        """Add one or more members to a sorted set, or update scores if they already exist"""
        if not key or score is None or member is None:
            return RedisError("wrong number of arguments for 'zadd' command")
        
        try:
            # Convert first score to float
            float(score) 
        except ValueError:
            return RedisError("value is not a valid float")
            
        # Verify additional args come in score-member pairs
        if len(args) % 2 != 0:
            return RedisError("syntax error: wrong number of arguments")
            
        # Try all DBs if we don't have connection context
        if 'connection' in locals():
            db_nums = [connection.db]
        else:
            db_nums = list(self.tables.keys())
            
        for db_num in db_nums:
            # Check if key exists and is a sorted set
            if await self.check_ttl(db_num, key) and key in self.tables[db_num]:
                data = self.tables[db_num][key]
                if not isinstance(data, SortedSet):
                    return BAD_VALUE
            else:
                # Create new sorted set
                data = SortedSet()
                self.tables[db_num][key] = data
                
            # Add first score-member pair
            added = 0
            if data.add(member, float(score)):
                added += 1
                
            # Add remaining score-member pairs
            for i in range(0, len(args), 2):
                try:
                    s = float(args[i])
                    m = args[i + 1]
                    if data.add(m, s):
                        added += 1
                except ValueError:
                    # Skip invalid scores but continue processing
                    continue
                    
            return added
            
        # Should not reach here if at least one DB exists
        return 0
            
    async def handle_zrange(self, key: str, start: str, stop: str, *args: str) -> list:
        """Return a range of members from a sorted set, by index"""
        if not key or start is None or stop is None:
            return RedisError("wrong number of arguments for 'zrange' command")
            
        try:
            start_idx = int(start)
            stop_idx = int(stop)
        except ValueError:
            return RedisError("value is not an integer")
            
        # Parse options
        withscores = False
        for arg in args:
            if arg.lower() == "withscores":
                withscores = True
                
        # Try all DBs if we don't have connection context
        task = asyncio.current_task()
        if task and hasattr(task, 'connection'):
            connection = getattr(task, 'connection')
            db_nums = [connection.db]
        else:
            db_nums = list(self.tables.keys())
            
        for db_num in db_nums:
            # Check if key exists
            if not await self.check_ttl(db_num, key) or key not in self.tables[db_num]:
                return []
                
            data = self.tables[db_num][key]
            if not isinstance(data, SortedSet):
                return BAD_VALUE
                
            # Handle negative indices like Redis
            length = len(data)
            if start_idx < 0:
                start_idx = length + start_idx
            if stop_idx < 0:
                stop_idx = length + stop_idx
                
            # Clamp indices
            start_idx = max(0, start_idx)
            stop_idx = min(length - 1, stop_idx)
            
            # Get the range including scores if requested
            if start_idx <= stop_idx:
                result = []
                items = data.range_by_rank(start_idx, stop_idx + 1)
                for member, score in items:
                    result.append(member)
                    if withscores:
                        result.append(str(score))
                return result
            return []
            
        return []  # Key does not exist

    async def start(self) -> None:
        """Start the Redis server."""
        log.info(f"Starting AsyncRedisServer on {self.host}:{self.port}")
        self._server = await asyncio.start_server(
            self.handle_client,
            self.host,
            self.port
        )
        
        # Create a background task to check for expired keys
        self._expiry_task = asyncio.create_task(self._check_expirations())
        
        # Create a background task to periodically save data
        self._save_task = asyncio.create_task(self._auto_save())
        
        addr = self._server.sockets[0].getsockname() if self._server.sockets else (self.host, self.port)
        log.info(f"AsyncRedisServer running on {addr[0]}:{addr[1]}")
        
    async def stop(self) -> None:
        """Stop the Redis server and close all connections."""
        log.info("Stopping async Redis server...")
        
        # Cancel background tasks
        if hasattr(self, '_expiry_task') and self._expiry_task:
            self._expiry_task.cancel()
            
        if hasattr(self, '_save_task') and self._save_task:
            self._save_task.cancel()
        
        # Save data before shutdown
        await self.save_data()
        
        # Close the server
        if self._server:
            self._server.close()
            await self._server.wait_closed()
            
        # Cancel any remaining client tasks
        for task in self._tasks:
            if not task.done():
                task.cancel()
                
        log.info("AsyncRedisServer stopped.")
        
    async def _check_expirations(self) -> None:
        """Background task to check for expired keys."""
        while True:
            try:
                # Check each timeout entry
                for key in list(self.timeouts.keys()):
                    try:
                        db_key = key.split(' ', 1)
                        if len(db_key) != 2:
                            continue
                            
                        db_num = int(db_key[0])
                        key_name = db_key[1]
                        
                        # If expired, remove the key
                        if self.timeouts[key] <= time.time():
                            if db_num in self.tables and key_name in self.tables[db_num]:
                                del self.tables[db_num][key_name]
                            del self.timeouts[key]
                    except (ValueError, KeyError):
                        # Skip invalid entries
                        continue
                
                # Sleep to avoid consuming too many resources
                await asyncio.sleep(0.1)
            except asyncio.CancelledError:
                # Clean exit on cancellation
                break
            except Exception as e:
                log.exception(f"Error in expiration check: {e}")
                # Continue running despite errors
                await asyncio.sleep(1)
                
    async def _auto_save(self) -> None:
        """Background task to periodically save data."""
        save_interval = 300  # Save every 5 minutes
        last_save = time.time()
        
        while True:
            try:
                current_time = time.time()
                if current_time - last_save >= save_interval:
                    await self.save_data()
                    last_save = current_time
                    
                # Sleep for a bit to avoid frequent checks
                await asyncio.sleep(10)
            except asyncio.CancelledError:
                # Clean exit on cancellation
                break
            except Exception as e:
                log.exception(f"Error in auto-save: {e}")
                # Continue running despite errors
                await asyncio.sleep(60)
