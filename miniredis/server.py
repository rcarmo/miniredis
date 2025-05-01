#!/usr/bin/env python
# encoding: utf-8
"""
Based on a minimalist Redis server originally written by Benjamin Pollack

First modified by Rui Carmo on 2013-03-12
Published under the MIT license.
"""

from collections.abc import Mapping
from collections import deque
import getopt  # Add missing import
import logging
import os
import re
import select
import signal
import socket
import sys
import threading
import time
from pathlib import Path
from random import choice, sample
from typing import Any, Dict, List, Optional, Set, Tuple, Union, TypeAlias

log = logging.getLogger()

from .haystack import Haystack


class RedisConstant:
    def __init__(self, type: str) -> None:
        self.type = type

    def __len__(self) -> int:
        return 0

    def __repr__(self) -> str:
        return f"<RedisConstant({self.type})>"


class RedisMessage:
    def __init__(self, message: str) -> None:
        self.message = message

    def __str__(self) -> str:
        return f"+{self.message}"

    def __repr__(self) -> str:
        return f"<RedisMessage({self.message})>"


class RedisError(RedisMessage):
    def __init__(self, message: str) -> None:
        self.message = message

    def __str__(self) -> str:
        return f"-ERR {self.message}"

    def __repr__(self) -> str:
        return f"<RedisError({self.message})>"


EMPTY_SCALAR = RedisConstant("EmptyScalar")
EMPTY_LIST = RedisConstant("EmptyList")
BAD_VALUE = RedisError("Operation against a key holding the wrong kind of value")


class RedisConnection:
    """Class to represent a client connection"""

    def __init__(self, socket: socket.socket) -> None:
        self.socket = socket
        self.wfile = socket.makefile("wb")
        self.rfile = socket.makefile("rb")
        self.db: int = 0
        self.table: Dict[str, Any] = {}


class RedisServer:
    def __init__(
        self, host: str = "127.0.0.1", port: int = 6379, db_path: str = "."
    ) -> None:
        super().__init__()
        self.host = host
        self.port = port
        self.halt = True
        self.clients: Dict[socket.socket, RedisConnection] = {}
        self.tables: Dict[int, Dict[str, Any]] = {}
        self.channels: Dict[str, List[RedisConnection]] = {}
        self.lastsave = int(time.time())
        self.path = db_path
        self.meta = Haystack(self.path, "redisdb")
        self.timeouts: Dict[str, float] = self.meta.get("timeouts", {})

    def dump(self, client: RedisConnection, o: Any) -> None:
        """Output a result to a client"""
        nl = b"\r\n"
        if isinstance(o, bool):
            if o:
                client.wfile.write(b"+OK\r\n")
        elif o == EMPTY_SCALAR:
            client.wfile.write(b"$-1\r\n")
        elif o == EMPTY_LIST:
            client.wfile.write(b"*-1\r\n")
        elif isinstance(o, int):
            client.wfile.write(b":" + str(o).encode() + nl)
        elif isinstance(o, str):
            o_bytes = o.encode()
            client.wfile.write(b"$" + str(len(o_bytes)).encode() + nl)
            client.wfile.write(o_bytes + nl)
        elif isinstance(o, bytes):
            client.wfile.write(b"$" + str(len(o)).encode() + nl)
            client.wfile.write(o + nl)
        elif isinstance(o, list):
            client.wfile.write(b"*" + str(len(o)).encode() + nl)
            for val in o:
                if isinstance(val, (str, bytes, int, float)):
                    self.dump(client, val)
                elif val is None:
                    self.dump(client, EMPTY_SCALAR)
                else:
                    self.dump(client, str(val))
        elif isinstance(o, RedisMessage):
            client.wfile.write(str(o).encode() + b"\r\n")
        elif isinstance(o, dict):
            client.wfile.write(b"*" + str(len(o) * 2).encode() + nl)
            for k, v in o.items():
                self.dump(client, str(k))
                self.dump(client, str(v) if v is not None else EMPTY_SCALAR)
        else:
            client.wfile.write(b"return type not yet implemented\r\n")
        client.wfile.flush()

    def log(self, client: Optional[RedisConnection], s: str) -> None:
        """Server logging"""
        try:
            who = (
                f"{client.socket.getpeername()[0]}:{client.socket.getpeername()[1]}"
                if client
                else "SERVER"
            )
        except:
            who = "<CLOSED>"
        log.debug(f"{who}: {s}")

    def handle(self, client: RedisConnection) -> None:
        """Handle commands"""

        keys_to_check = (
            sample(list(self.timeouts.keys()), len(self.timeouts) // 4)
            if self.timeouts
            else []
        )
        for e in keys_to_check:
            self.check_ttl(client, e.split(" ", 1)[1])

        line = client.rfile.readline()
        if not line:
            self.log(client, "client disconnected")
            del self.clients[client.socket]
            client.socket.close()
            return
        items = int(line[1:].strip())
        args = []
        for _ in range(items):
            length_line = client.rfile.readline().strip()
            if not length_line or not length_line.startswith(b"$"):
                raise RedisError("Protocol error: expected bulk string length")
            length = int(length_line[1:])
            if length == -1:
                args.append(None)
            else:
                data = client.rfile.read(length)
                if len(data) < length:
                    raise RedisError("Protocol error: insufficient data read")
                args.append(data)
                crlf = client.rfile.read(2)
                if crlf != b"\r\n":
                    raise RedisError("Protocol error: expected CRLF")

        try:
            command = args[0].decode("utf-8").lower()
            decoded_args = []
            for arg in args[1:]:
                if arg is not None:
                    decoded_args.append(arg.decode("utf-8"))
                else:
                    decoded_args.append(None)
        except UnicodeDecodeError:
            raise RedisError("Command or arguments not valid UTF-8")

        handler_name = "handle_" + command
        if hasattr(self, handler_name):
            self.dump(client, getattr(self, handler_name)(client, *decoded_args))
        else:
            self.dump(client, RedisError(f"unknown command '{command}'"))

    def rotate(self) -> None:
        """Rotate log file using context manager for better resource handling"""
        try:
            self.log_file.close()
            with open(self.log_name, "w") as new_log_file:
                self.log_file = new_log_file
        except (FileNotFoundError, PermissionError) as e:
            log.error(f"Error rotating log file: {e}")

    def run(self) -> None:
        """Main loop for standard socket handling with improved exception handling"""
        self.halt = False
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            server.bind((self.host, self.port))
            server.listen(5)
            while not self.halt:
                try:
                    readable, _, _ = select.select(
                        [server] + list(self.clients.keys()), [], [], 1.0
                    )
                except select.error as e:
                    if hasattr(socket, "errno") and e.args[0] == socket.errno.EINTR:
                        continue
                    raise
                except (ValueError, TypeError) as e:
                    log.error(f"Error in select: {e}")
                    continue

                for sock in readable:
                    if sock == server:
                        try:
                            (client_socket, address) = server.accept()
                            client = RedisConnection(client_socket)
                            self.clients[client_socket] = client
                            self.log(client, "client connected")
                            self.select(client, 0)
                        except OSError as e:
                            log.error(f"Error accepting connection: {e}")
                    else:
                        client = self.clients.get(sock)
                        if client:
                            try:
                                self.handle(client)
                            except (
                                socket.error,
                                ConnectionError,
                                EOFError,
                                ConnectionResetError,
                            ) as e:
                                self.log(client, f"client connection error: {e}")
                                self.handle_quit(client)
                            except Exception as e:
                                self.log(client, f"exception: {e}")
                                self.handle_quit(client)
        finally:
            # Ensure proper cleanup on exit
            for client_socket in list(self.clients.keys()):
                try:
                    client_socket.close()
                except socket.error:
                    pass
            self.clients.clear()
            server.close()

    def save(self) -> None:
        """Serialize tables to disk"""
        self.meta["timeouts"] = self.timeouts
        for db in self.tables:
            self.meta[db] = self.tables[db]
        self.meta.commit()
        self.lastsave = int(time.time())

    def select(self, client: RedisConnection, db: int) -> None:
        if db not in self.tables:
            self.tables[db] = self.meta.get(db, {})
        client.db = db
        client.table = self.tables[db]

    def stop(self) -> None:
        if not self.halt:
            self.log(None, "STOPPING")
            self.save()
            self.halt = True

    def check_ttl(self, client: RedisConnection, key: str) -> None:
        k = f"{client.db} {key}"
        if k in self.timeouts:
            if self.timeouts[k] <= time.time():
                self.handle_del(client, key)

    # command handlers, sorted by order of redis.io docs

    # Keys

    def handle_del(self, client, *args):
        count = 0
        for key in args:
            self.handle_persist(client, key)
            self.log(client, f"DEL {key}")
            if key not in client.table:
                continue
            del client.table[key]
            count += 1
        return count

    def handle_dump(self, client, key):
        self.log(client, f"DUMP {key}")
        # no special internal representation
        return str(client.table[key])

    def handle_exists(self, client, key):
        self.check_ttl(client, key)
        if key in client.table:
            return 1
        return 0

    def handle_expire(self, client, key, ttl):
        ttl = int(ttl)
        self.log(client, f"EXPIRE {key} {ttl}")
        if key not in client.table:
            return 0
        self.timeouts[f"{client.db} {key}"] = time.time() + ttl
        return 1

    def handle_expireat(self, client, key, when):
        when = int(when)
        self.log(client, f"EXPIREAT {key} {when}")
        if key not in client.table:
            return 0
        self.timeouts[f"{client.db} {key}"] = when
        return 1

    def handle_keys(self, client, pattern):
        r = re.compile("^" + pattern.replace("*", ".*") + "$")
        self.log(client, f"KEYS {pattern}")
        return [k for k in client.table.keys() if r.match(k)]

    # def handle_migrate(self, client, host, port, key, db, timeout, option):

    def handle_move(self, client, key, db):
        self.log(client, f"MOVE {key}")
        if key not in client.table:
            return 0
        self.handle_persist(client, key)
        if db not in self.tables:
            self.tables[db] = self.meta.get(db, {})
        if key in self.tables[db]:
            return 0
        self.tables[db][key] = client.table[key]
        del client.table[key]
        return 1

    # def handle_object(self, client, subcommand, *args)

    def handle_persist(self, client, key):
        try:
            del self.timeouts[f"{client.db} {key}"]
        except:
            pass

    def handle_pexpire(self, client, key, mttl):
        mttl = int(mttl)
        if key not in client.table:
            return 0
        self.timeouts[f"{client.db} {key}"] = time.time() + (mttl / 1000)
        return 1

    def handle_pexpireat(self, client, key, mwhen):
        mwhen = int(mwhen)
        if key not in client.table:
            return 0
        self.timeouts[f"{client.db} {key}"] = mwhen / 1000
        return 1

    def handle_pttl(self, client, key):
        self.log(client, f"PTTL {key}")
        if key not in client.table:
            return -2
        k = f"{client.db} {key}"
        if k not in self.timeouts:
            return -1
        return int(self.timeouts[k] * 1000)

    def handle_randomkey(self, client):
        self.log(client, "RANDOMKEY")
        if len(client.table):
            return self.get(client, choice(list(client.table.keys())))
        return 0

    def handle_rename(self, client, key, newkey):
        client.table[newkey] = client.table[key]
        k = f"{client.db} {key}"
        # transfer TTL
        if k in self.timeouts:
            self.timeouts[f"{client.db} {key}"] = self.timeouts[k]
            del self.timeouts[k]
        del client.table[key]
        self.log(client, f"RENAME {key} -> {newkey}")
        return True

    def handle_renamenx(self, client, key, newkey):
        self.log(client, f"RENAMENX {key} -> {newkey}")
        if newkey not in client.table:
            self.handle_rename(client, key, newkey)
            return 1
        return 0

    # def handle_sort(self, client, key, *args)

    def handle_ttl(self, client, key):
        if key not in client.table:
            return -2
        k = f"{client.db} {key}"
        if k not in self.timeouts:
            return -1
        return int(self.timeouts[k] - time.time() + 0.1)

    def handle_type(self, client, key):
        if key not in client.table:
            return RedisMessage("none")

        data = client.table[key]
        if isinstance(data, deque):
            return RedisMessage("list")
        elif isinstance(data, set):
            return RedisMessage("set")
        elif isinstance(data, dict):
            return RedisMessage("hash")
        elif isinstance(data, str):
            return RedisMessage("string")
        else:
            return RedisError("unknown data type")

    # def handle_scan(self, client, *args)

    # Strings

    def handle_append(self, client, key, value):
        if key not in client.table:
            self.handle_set(client, key, value)
            return len(client.table[key])
        data = client.table[key]
        if isinstance(data, str):
            self.handle_persist(client, key)
            client.table[key] += value
            self.log(client, f"APPEND {key} -> {len(client.table[key])}")
            return len(client.table[key])
        return BAD_VALUE

    # def handle_bitcount(self, client, key, start, end)
    # def handle_bitop(self, client, *args)

    def handle_decr(self, client, key):
        self.check_ttl(client, key)
        return self.handle_decrby(client, key, 1)

    def handle_decrby(self, client, key, by):
        self.check_ttl(client, key)
        return self.handle_incrby(client, key, -int(by))

    def handle_get(self, client, key):
        self.check_ttl(client, key)
        data = client.table.get(key, None)
        if isinstance(data, deque):
            return BAD_VALUE
        if data is not None:
            data = str(data)
        else:
            data = EMPTY_SCALAR
        self.log(client, f"GET {key} -> {len(data) if data != EMPTY_SCALAR else 0}")
        return data

    # def handle_getbit(self, client, key, offset):
    # def handle_getrange(self, client, key, start, end):

    def handle_getset(self, client, key, data):
        self.handle_persist(client, key)
        old_data = client.table.get(key, None)
        if isinstance(old_data, deque):
            return BAD_VALUE
        if old_data is not None:
            old_data = str(old_data)
        else:
            old_data = EMPTY_SCALAR
        client.table[key] = data
        self.log(client, f"GETSET {key} {data} -> {old_data}")
        return old_data

    def handle_incr(self, client, key):
        self.check_ttl(client, key)
        return self.handle_incrby(client, key, 1)

    def handle_incrby(self, client, key, by):
        self.check_ttl(client, key)
        try:
            client.table[key] = int(client.table[key])
            client.table[key] += int(by)
        except (KeyError, TypeError, ValueError):
            client.table[key] = 1
        self.log(client, f"INCRBY {key} {by} -> {client.table[key]}")
        return client.table[key]

    # def handle_incrbyfloat(self, client, key, by):

    def handle_mget(self, client, *keys):
        result = []
        for k in keys:
            self.check_ttl(client, k)
            data = client.table.get(k, None)
            if isinstance(data, deque):
                return BAD_VALUE
            if data is not None:
                data = str(data)
            else:
                data = EMPTY_SCALAR
            result.append(data)
        self.log(client, f"MGET {keys} -> {result}")
        return result

    # def handle_mset(self, client, *args):
    # def handle_msetnx(self, client, *args):
    # def handle_psetex(self, client, key, ms, value):

    def handle_set(self, client, key, data):
        self.handle_persist(client, key)
        client.table[key] = data
        self.log(client, f"SET {key} -> {len(data)}")
        return True

    # def handle_setbit(self, client, key, offset, value)

    def handle_setex(self, client, key, seconds, data):
        self.handle_set(client, key, data)
        return self.handle_expire(client, key, seconds)

    def handle_setnx(self, client, key, data):
        if key in client.table:
            self.log(client, f"SETNX {key} -> {len(data)} FAILED")
            return 0
        client.table[key] = data
        self.log(client, f"SETNX {key} -> {len(data)}")
        return 1

    # def handle_setrange(self, client, key, offset, value)
    # def handle_strlen(self, client, key)

    # Hashes

    # Lists

    # def handle_blpop(self, client, *args)
    # def handle_brpop(self, client, *args)
    # def handle_brpoplpush(self, client, *args)

    # def handle_lindex(self, client, key, index)
    # def handle_linsert(self, client, key, *args)

    def handle_llen(self, client, key):
        self.check_ttl(client, key)
        if key not in client.table:
            return 0
        if not isinstance(client.table[key], deque):
            return BAD_VALUE
        return len(client.table[key])

    def handle_lpop(self, client, key):
        self.check_ttl(client, key)
        if key not in client.table:
            return EMPTY_SCALAR
        if not isinstance(client.table[key], deque):
            return BAD_VALUE
        if len(client.table[key]) > 0:
            data = client.table[key].popleft()
        else:
            data = EMPTY_SCALAR
        self.log(client, f"LPOP {key} -> {data}")
        return data

    def handle_lpush(self, client, key, data):
        self.check_ttl(client, key)
        if key not in client.table:
            client.table[key] = deque()
        elif not isinstance(client.table[key], deque):
            return BAD_VALUE
        client.table[key].appendleft(data)
        self.log(client, f"LPUSH {key} {data}")
        return True

    # def handle_lpushx(self, client, key, data):

    def handle_lrange(self, client, key, start, stop):
        self.check_ttl(client, key)
        start, stop = int(start), int(stop)
        if start == 0 and stop == -1:
            stop = None
        if key not in client.table:
            return EMPTY_LIST
        if not isinstance(client.table[key], deque):
            return BAD_VALUE
        l = list(client.table[key])[start:stop]
        self.log(client, f"LRANGE {key} {start} {stop} -> {l}")
        return l

    # def handle_lrem(self, client, key, start, stop):
    # def handle_lset(self, client, key, index, value):
    # def handle_ltrim(self, client, key, start, stop):

    def handle_rpop(self, client, key):
        self.check_ttl(client, key)
        if key not in client.table:
            return EMPTY_SCALAR
        if not isinstance(client.table[key], deque):
            return BAD_VALUE
        if len(client.table[key]) > 0:
            data = client.table[key].pop()
        else:
            data = EMPTY_SCALAR
        self.log(client, f"RPOP {key} -> {data}")
        return data

    # def handle_rpoplpush(self, source, destination)

    def handle_rpush(self, client, key, data):
        self.check_ttl(client, key)
        if key not in client.table:
            client.table[key] = deque()
        elif not isinstance(client.table[key], deque):
            return BAD_VALUE
        client.table[key].append(data)
        self.log(client, f"RPUSH {key} {data}")
        return True

    # def handle_rpushx(self, client, key, data)

    # Hashes (TODO: add type checks)

    def handle_hdel(self, client, key, *keys):
        if key not in client.table:
            return 0
        self.check_ttl(client, key)
        count = 0
        for field in keys:
            if field in client.table[key]:
                del client.table[key][field]
                count += 1
        return count

    def handle_hexists(self, client, key, field):
        if key not in client.table:
            return 0
        self.check_ttl(client, key)
        return 1 if field in client.table[key] else 0

    def handle_hget(self, client, key, field):
        if key not in client.table:
            return 0
        self.check_ttl(client, key)
        return client.table[key][field] if field in client.table[key] else None

    def handle_hgetall(self, client, key):
        self.check_ttl(client, key)
        try:
            return client.table[key]
        except:
            return []

    def handle_hincrby(self, client, key, field, increment):
        increment = int(increment)  # Convert to int for Python 3
        if key not in client.table:
            client.table[key] = {}
        prev = int(client.table[key].get(field, "0"))

        client.table[key][field] = str(prev + increment)
        return client.table[key][field]

    # def handle_hincrbyfloat(self, client, key, field, increment):

    def handle_hkeys(self, client, key):
        if key not in client.table:
            return []
        return list(client.table[key].keys())  # Convert keys view to list

    def handle_hlen(self, client, key):
        self.check_ttl(client, key)
        return len(client.table[key])

    def handle_hmget(self, client, key, *fields):
        self.check_ttl(client, key)
        return [client.table[key].get(f) for f in fields]

    def handle_hmset(self, client, key, items):
        self.check_ttl(client, key)
        for k, v in items.items():
            client.table[key][k] = v  # Use direct indexing
        return True

    def handle_hset(self, client, key, field, value):
        self.check_ttl(client, key)
        if key not in client.table:
            client.table[key] = {}
        if field not in client.table[key]:
            client.table[key][field] = value
            return 1
        client.table[key][field] = value
        return 0

    # def handle_hsetnx(self, client, key, field, value)

    def handle_hvals(self, client, key):
        if key not in client.table:
            return []
        return list(client.table[key].values())  # Convert values view to list

    # def hscan(self, client, key, cursor, *args)

    # Server

    def handle_bgsave(self, client):
        if hasattr(os, "fork"):
            if not os.fork():
                self.save()
                sys.exit(0)
        else:
            self.save()
        self.log(client, "BGSAVE")
        return RedisMessage("Background saving started")

    def handle_flushdb(self, client):
        self.log(client, "FLUSHDB")
        client.table.clear()
        return True

    def handle_flushall(self, client):
        self.log(client, "FLUSHALL")
        for table in self.tables.values():  # Use values() instead of itervalues()
            table.clear()
        return True

    def handle_lastsave(self, client):
        return self.lastsave

    def handle_ping(self, client):
        self.log(client, "PING -> PONG")
        return RedisMessage("PONG")

    def handle_quit(self, client):
        try:
            client.socket.shutdown(socket.SHUT_RDWR)
        except:
            pass  # Socket might already be closed
        client.socket.close()
        self.log(client, "QUIT")
        if client.socket in self.clients:
            del self.clients[client.socket]
        return False

    def handle_save(self, client):
        self.save()
        self.log(client, "SAVE")
        return True

    def handle_select(self, client, db):
        db = int(db)
        self.select(client, db)
        self.log(client, f"SELECT {db}")
        return True

    # PubSub

    def handle_publish(self, client, channel, message):
        count = 0
        for p in self.channels.keys():
            if re.match(p, channel):
                for c in self.channels[channel]:
                    c.wfile.write(b"*3\r\n")
                    c.wfile.write(f'${len("message")}\r\n'.encode())
                    c.wfile.write(b"message\r\n")
                    c.wfile.write(f"${len(channel)}\r\n".encode())
                    c.wfile.write(channel.encode() + b"\r\n")
                    c.wfile.write(f"${len(message)}\r\n".encode())
                    c.wfile.write(message.encode() + b"\r\n")
                    count += 1
        return count

    def handle_subscribe(self, client, *channels):
        count = 0
        for c in channels:
            if c not in self.channels:
                self.channels[c] = []
            self.channels[c].append(client)
            count += 1
        return count

    def handle_unsubscribe(self, client, *channels):
        # If no channels provided, unsubscribe from all
        if not channels:
            channels_to_remove = []
            for c, clients in self.channels.items():
                if client in clients:
                    clients.remove(client)
                    if not clients:
                        channels_to_remove.append(c)

            # Remove empty channel entries
            for c in channels_to_remove:
                del self.channels[c]
            return True

        # Unsubscribe from specified channels
        count = 0
        for c in channels:
            try:
                if c in self.channels and client in self.channels[c]:
                    self.channels[c].remove(client)
                    count += 1
                    if not self.channels[c]:  # Clean up empty lists
                        del self.channels[c]
            except:
                pass
        return count

    def handle_psubscribe(self, client, *patterns):
        # Similar implementation to handle_subscribe but for pattern subscriptions
        count = 0
        for pattern in patterns:
            if pattern not in self.channels:
                self.channels[pattern] = []
            self.channels[pattern].append(client)
            count += 1
        return count

    def handle_punsubscribe(self, client, *patterns):
        # If no patterns provided, unsubscribe from all patterns
        if not patterns:
            patterns_to_remove = []
            for p, clients in self.channels.items():
                # We only want to remove from patterns, not regular channels
                # This is a simplification, in real Redis this is more complex
                if p.startswith("*") or p.startswith("?"):
                    if client in clients:
                        clients.remove(client)
                        if not clients:
                            patterns_to_remove.append(p)

            # Remove empty pattern entries
            for p in patterns_to_remove:
                del self.channels[p]
            return True

        # Unsubscribe from specified patterns
        count = 0
        for p in patterns:
            try:
                if p in self.channels and client in self.channels[p]:
                    self.channels[p].remove(client)
                    count += 1
                    if not self.channels[p]:  # Clean up empty lists
                        del self.channels[p]
            except:
                pass
        return count

    def handle_shutdown(self, client):
        self.log(client, "SHUTDOWN")
        self.halt = True
        self.save()
        return self.handle_quit(client)


class ThreadedRedisServer(RedisServer):
    """
    # for use in an accept() loop:
    import threading, socket
    sock = socket(AF_INET, SOCK_STREAM)
    sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
    sock.bind(('127.0.0.1', port=6379))
    serversock.listen(5)
    t = ThreadedRedisServer()
    while True:
        clientsock, addr = sock.accept()
        thread = threading.Thread(target=t.thread, args=(clientsock, addr))
        thread.daemon = True
        thread.start()
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def thread(self, sock, address):
        client = RedisConnection(sock)
        self.clients[sock] = client
        self.log(client, "client connected")
        self.select(client, 0)
        while not self.halt:
            try:
                self.handle(self.clients[sock])
            except (socket.error, EOFError, ConnectionResetError) as e:
                self.log(client, f"client connection error in thread: {e}")
                break
            except Exception as e:
                self.log(client, f"exception in thread: {e}")
                break
        try:
            if sock in self.clients:
                self.handle_quit(client)
        except Exception as e:
            log.debug(f"Error during quit in thread: {e}")
            if sock in self.clients:
                del self.clients[sock]
            try:
                sock.close()
            except socket.error:
                pass


def fork(**kwargs):
    if not hasattr(os, "fork"):
        print(
            "Fork not supported on this OS. Consider using multiprocessing or threading.",
            file=sys.stderr,
        )
        sys.exit(1)

    try:
        pid = os.fork()
        if pid > 0:
            return pid
        m = RedisServer(**kwargs)
        m.run()
        sys.exit(0)
    except KeyboardInterrupt:
        pass
    except OSError as e:
        print(
            f"Failed to launch Redis subprocess: {e.errno} ({e.strerror})",
            file=sys.stderr,
        )
        sys.exit(1)


def main(args):
    global m
    m = None

    if os.name == "posix":

        def sigterm_handler(signum, frame):
            if m:
                m.stop()
            sys.exit(0)

        def sighup_handler(signum, frame):
            if m:
                m.rotate()

        signal.signal(signal.SIGTERM, sigterm_handler)
        signal.signal(signal.SIGHUP, sighup_handler)

    host, port, log_file, db_path = "127.0.0.1", 6379, None, "."
    pid_file = None
    try:
        opts, args = getopt.getopt(args, "h:p:d:l:f:")
    except getopt.GetoptError as err:
        print(str(err), file=sys.stderr)
        sys.exit(2)

    for o, a in opts:
        if o == "-h":
            host = a
        elif o == "-p":
            port = int(a)
        elif o == "-l":
            log_file = os.path.abspath(a)
        elif o == "-d":
            db_path = os.path.abspath(a)
        elif o == "-f":
            pid_file = os.path.abspath(a)

    log_level = logging.INFO
    log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    if log_file:
        logging.basicConfig(filename=log_file, level=log_level, format=log_format)
    else:
        logging.basicConfig(level=log_level, format=log_format)

    if pid_file:
        try:
            with open(pid_file, "w") as f:
                f.write(f"{os.getpid()}\n")
        except IOError as e:
            log.error(f"Could not write PID file {pid_file}: {e}")
            pid_file = None

    m = RedisServer(host=host, port=port, db_path=db_path)
    log.info(f"Starting miniredis server on {host}:{port}, DB path: {db_path}")
    try:
        m.run()
    except KeyboardInterrupt:
        log.info("KeyboardInterrupt received, stopping server.")
        if m:
            m.stop()
    except Exception as e:
        log.exception("Unhandled exception in server run loop")
        if m:
            m.stop()
    finally:
        if pid_file and os.path.exists(pid_file):
            try:
                os.unlink(pid_file)
            except OSError as e:
                log.error(f"Could not remove PID file {pid_file}: {e}")
        log.info("Server stopped.")
    sys.exit(0)


if __name__ == "__main__":
    main(sys.argv[1:])
