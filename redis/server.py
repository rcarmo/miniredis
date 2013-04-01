#!/usr/bin/env python
# encoding: utf-8
"""
Based on a minimalist Redis server originally written by Benjamin Pollack

First modified by Rui Carmo on 2013-03-12
Published under the MIT license.
"""

from __future__ import with_statement
import os, sys, time, logging
import socket, select, thread, signal, errno
import re, getopt
from collections import deque
from haystack import Haystack

log = logging.getLogger()

class RedisConstant(object):
    def __init__(self, type):
        self.type = type

    def __repr__(self):
        return '<RedisConstant(%s)>' % self.type

class RedisMessage(object):
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return '+%s' % self.message

    def __repr__(self):
        return '<RedisMessage(%s)>' % self.message

class RedisError(RedisMessage):
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return '-ERR %s' % self.message

    def __repr__(self):
        return '<RedisError(%s)>' % self.message


EMPTY_SCALAR = RedisConstant('EmptyScalar')
EMPTY_LIST = RedisConstant('EmptyList')
BAD_VALUE = RedisError('Operation against a key holding the wrong kind of value')


class RedisClient(object):
    def __init__(self, socket):
        self.socket = socket
        self.wfile = socket.makefile('wb')
        self.rfile = socket.makefile('rb')
        self.db = None
        self.table = None


class RedisServer(object):
    def __init__(self, host='127.0.0.1', port=6379, db_path='.'):
        super(RedisServer, self).__init__()
        self.host = host
        self.port = port
        self.halt = True
        self.clients = {}
        self.tables = {}
        self.channels = {}
        self.lastsave = int(time.time())
        self.path = db_path
        self.meta = Haystack(self.path,'meta')


    def dump(self, client, o):
        nl = '\r\n'
        if isinstance(o, bool):
            if o:
                client.wfile.write('+OK\r\n')
            # Show nothing for a false return; that means be quiet
        elif o == EMPTY_SCALAR:
            client.wfile.write('$-1\r\n')
        elif o == EMPTY_LIST:
            client.wfile.write('*-1\r\n')
        elif isinstance(o, int):
            client.wfile.write(':' + str(o) + nl)
        elif isinstance(o, str):
            client.wfile.write('$' + str(len(o)) + nl)
            client.wfile.write(o + nl)
        elif isinstance(o, list):
            client.wfile.write('*' + str(len(o)) + nl)
            for val in o:
                self.dump(client, str(val))
        elif isinstance(o, RedisMessage):
            client.wfile.write('%s\r\n' % o)
        else:
            client.wfile.write('return type not yet implemented\r\n')
        client.wfile.flush()


    def log(self, client, s):
        try:
            who = '%s:%s' % client.socket.getpeername() if client else 'SERVER'
        except:
            who = '<CLOSED>'
        log.debug("%s: %s" % (who, s))


    def handle(self, client):
        line = client.rfile.readline()
        if not line:
            log.debug(client, 'client disconnected')
            del self.clients[client.socket]
            client.socket.close()
            return
        items = int(line[1:].strip())
        args = []
        for x in xrange(0, items):
            length = int(client.rfile.readline().strip()[1:])
            args.append(client.rfile.read(length))
            client.rfile.read(2) # throw out newline
        command = args[0].lower()
        self.dump(client, getattr(self, 'handle_' + command)(client, *args[1:]))


    def rotate(self):
        log.debug_file.close()
        log.debug_file = open(log.debug_name, 'w')


    def run(self):
        self.halt = False
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((self.host, self.port))
        server.listen(5)
        while not self.halt:
            try:
                readable, _, _ = select.select([server] + self.clients.keys(), [], [], 1.0)
            except select.error, e:
                if e.args[0] == errno.EINTR:
                    continue
                raise
            for sock in readable:
                if sock == server:
                    (client_socket, address) = server.accept()
                    client = RedisClient(client_socket)
                    self.clients[client_socket] = client
                    log.debug(client, 'client connected')
                    self.select(client, 0)
                else:
                    try:
                        self.handle(self.clients[sock])
                    except Exception, e:
                        log.debug(client, 'exception: %s' % e)
                        self.handle_quit(client)
        for client_socket in self.clients.iterkeys():
            client_socket.close()
        self.clients.clear()
        server.close()


    def save(self):
        for db in self.tables:
            if db == 0:
                self.meta['0'] = self.tables[0]
                self.meta.commit()
            else:
                self.tables[db].commit()
        self.lastsave = int(time.time())


    def select(self, client, db):
        if db not in self.tables:
            if db == 0:
                self.tables[db] = {}
                self.tables[db].update(self.meta.get('0',{}))
            else:
                self.tables[db] = Haystack(self.path,str(db))
        client.db = db
        client.table = self.tables[db]


    def stop(self):
        if not self.halt:
            log.debug(None, 'STOPPING')
            self.save()
            self.halt = True


    def handle_bgsave(self, client):
        if hasattr(os, 'fork'):
            if not os.fork():
                self.save()
                sys.exit(0)
        else:
            self.save()
        log.debug(client, 'BGSAVE')
        return RedisMessage('Background saving started')


    def handle_decr(self, client, key):
        return self.handle_decrby(self, client, key, 1)


    def handle_decrby(self, client, key, by):
        return self.handle_incrby(self, client, key, -by)


    def handle_del(self, client, key):
        log.debug(client, 'DEL %s' % key)
        if key not in client.table:
            return 0
        del client.table[key]
        return 1


    def handle_flushdb(self, client):
        log.debug(client, 'FLUSHDB')
        client.table.clear()
        return True


    def handle_flushall(self, client):
        log.debug(client, 'FLUSHALL')
        for table in self.tables.itervalues():
            table.clear()
        return True


    def handle_get(self, client, key):
        data = client.table.get(key, None)
        if isinstance(data, deque):
            return BAD_VALUE
        if data != None:
            data = str(data)
        else:
            data = EMPTY_SCALAR
        log.debug(client, 'GET %s -> %s' % (key, data))
        return data


    def handle_incr(self, client, key):
        return self.handle_incrby(client, key, 1)


    def handle_incrby(self, client, key, by):
        try:
            client.table[key] = int(client.table[key])
            client.table[key] += int(by)
        except (KeyError, TypeError, ValueError):
            client.table[key] = 1
        log.debug(client, 'INCRBY %s %s -> %s' % (key, by, client.table[key]))
        return client.table[key]


    def handle_keys(self, client, pattern):
        r = re.compile(pattern.replace('*', '.*'))
        log.debug(client, 'KEYS %s' % pattern)
        return [k for k in client.table.keys() if r.search(k)]


    def handle_lastsave(self, client):
        return self.lastsave


    def handle_llen(self, client, key):
        if key not in client.table:
            return 0
        if not isinstance(client.table[key], deque):
            return BAD_VALUE
        return len(client.table[key])


    def handle_lpop(self, client, key):
        if key not in client.table:
            return EMPTY_SCALAR
        if not isinstance(client.table[key], deque):
            return BAD_VALUE
        if len(client.table[key]) > 0:
            data = client.table[key].popleft()
        else:
            data = EMPTY_SCALAR
        log.debug(client, 'LPOP %s -> %s' % (key, data))
        return data


    def handle_lpush(self, client, key, data):
        if key not in client.table:
            client.table[key] = deque()
        elif not isinstance(client.table[key], deque):
            return BAD_VALUE
        client.table[key].appendleft(data)
        log.debug(client, 'LPUSH %s %s' % (key, data))
        return True


    def handle_lrange(self, client, key, low, high):
        low, high = int(low), int(high)
        if low == 0 and high == -1:
            high = None
        if key not in client.table:
            return EMPTY_LIST
        if not isinstance(client.table[key], deque):
            return BAD_VALUE
        l = list(client.table[key])[low:high]
        log.debug(client, 'LRANGE %s %s %s -> %s' % (key, low, high, l))
        return l


    def handle_ping(self, client):
        log.debug(client, 'PING -> PONG')
        return RedisMessage('PONG')


    def handle_rpop(self, client, key):
        if key not in client.table:
            return EMPTY_SCALAR
        if not isinstance(client.table[key], deque):
            return BAD_VALUE
        if len(client.table[key]) > 0:
            data = client.table[key].pop()
        else:
            data = EMPTY_SCALAR
        log.debug(client, 'RPOP %s -> %s' % (key, data))
        return data


    def handle_rpush(self, client, key, data):
        if key not in client.table:
            client.table[key] = deque()
        elif not isinstance(client.table[key], deque):
            return BAD_VALUE
        client.table[key].append(data)
        log.debug(client, 'RPUSH %s %s' % (key, data))
        return True


    def handle_type(self, client, key):
        if key not in client.table:
            return RedisMessage('none')

        data = client.table[key]
        if isinstance(data, deque):
            return RedisMessage('list')
        elif isinstance(data, set):
            return RedisMessage('set')
        elif isinstance(data, dict):
            return RedisMessage('hash')
        elif isinstance(data, str):
            return RedisMessage('string')
        else:
            return RedisError('unknown data type')


    def handle_quit(self, client):
        client.socket.shutdown(socket.SHUT_RDWR)
        client.socket.close()
        log.debug(client, 'QUIT')
        del self.clients[client.socket]
        return False


    def handle_save(self, client):
        self.save()
        log.debug(client, 'SAVE')
        return True


    def handle_select(self, client, db):
        db = int(db)
        self.select(client, db)
        log.debug(client, 'SELECT %s' % db)
        return True


    def handle_set(self, client, key, data):
        client.table[key] = data
        log.debug(client, 'SET %s -> %s' % (key, data))
        return True


    def handle_setnx(self, client, key, data):
        if key in client.table:
            log.debug(client, 'SETNX %s -> %s FAILED' % (key, data))
            return 0
        client.table[key] = data
        log.debug(client, 'SETNX %s -> %s' % (key, data))
        return 1
    

    def handle_getset(self, client, key, data):
        old_data = client.table.get(key, None)
        if isinstance(old_data, deque):
            return BAD_VALUE
        if old_data != None:
            old_data = str(old_data)
        else:
            old_data = EMPTY_SCALAR
        client.table[key] = data
        log.debug(client, 'GETSET %s %s -> %s' % (key, data, old_data))
        return old_data


    def handle_publish(self, client, channel, message):
        for p in self.channels.keys():
            if re.match(p, channel):
                for c in self.channels[channel]:
                    c.wfile.write('*3\r\n')
                    c.wfile.write('$%d\r\n' % len('message'))
                    c.wfile.write('message\r\n')
                    c.wfile.write('$%d\r\n' % len(channel))
                    c.wfile.write(channel + '\r\n')
                    c.wfile.write('$%d\r\n' % len(message))
                    c.wfile.write(message + '\r\n')
        return True


    def handle_subscribe(self, client, channels):
        for c in channels.trim().split(' '):
            if c not in self.channels.keys():
                self.channels[c] = []
            self.channels[c].append(client)
        return True


    def handle_unsubscribe(self, client, channels):
        # TODO: handle no args (full unsubscribe)
        for c in channels.trim().split(' '):
            try:
                self.channels[c].remove(client)
            except:
                return False
        return True


    def handle_psubscribe(self, client, patterns):
        pass


    def handle_punsubscribe(self, client, patterns):
        pass


    def handle_shutdown(self, client):
        log.debug(client, 'SHUTDOWN')
        self.halt = True
        self.save()
        return self.handle_quit(client)


def main(args):
    if os.name == 'posix':
        def sigterm(signum, frame):
            m.stop()
        def sighup(signum, frame):
            m.rotate()
        signal.signal(signal.SIGTERM, sigterm)
        signal.signal(signal.SIGHUP, sighup)

    host, port, db_path = '127.0.0.1', 6379, '.'
    opts, args = getopt.getopt(args, 'h:p:d:f:')
    pid_file = None
    for o, a in opts:
        if o == '-h':
            host = a
        elif o == '-p':
            port = int(a)
        elif o == '-d':
            db_file = os.path.abspath(a)
        elif o == '-f':
            pid_file = os.path.abspath(a)
    if pid_file:
        with open(pid_file, 'w') as f:
            f.write('%s\n' % os.getpid())
    m = RedisServer(host=host, port=port, db_path=db_path)
    try:
        m.run()
    except KeyboardInterrupt:
        m.stop()
    if pid_file:
        os.unlink(pid_file)
    sys.exit(0)

if __name__ == '__main__':
    main(sys.argv[1:])
