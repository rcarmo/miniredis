#!/usr/bin/env python
# encoding: utf-8
"""
Based on a minimalist Redis server originally written by Benjamin Pollack

First modified by Rui Carmo on 2013-03-12
Published under the MIT license.
"""

from __future__ import with_statement
from collections import deque
import os, sys, time, logging, signal, getopt
import socket, select, thread, errno
from random import sample

log = logging.getLogger()

from .haystack import Haystack

class RedisConstant(object):
    def __init__(self, type):
        self.type = type

    def __len__(self):
        return 0

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
    """Class to represent a client connection"""
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
        self.meta = Haystack(self.path,'redisdb')
        self.expiries = self.meta.get('expiries',{})


    def dump(self, client, o):
        """Output a result to a client"""
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
        """Server logging"""
        try:
            who = '%s:%s' % client.socket.getpeername() if client else 'SERVER'
        except:
            who = '<CLOSED>'
        log.debug("%s: %s" % (who, s))


    def handle(self, client):
        """Handle commands"""

        # check 25% of keys, like "real" Redis
        for e in [k for k in sample(self.expiries.keys(), len(self.expiries.keys())/4)]:
            self.check_ttl(client, e)

        line = client.rfile.readline()
        if not line:
            self.log(client, 'client disconnected')
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


    def gevent_handler(self, client_socket, address):
        """gevent Streamserver handler"""
        client = RedisClient(client_socket)
        self.clients[client_socket] = client
        self.log(client, 'client connected')
        self.select(client,0)
        self.log(client, 'Entering loop.')
        while not self.halt:
            self.log(client, 'Handling...')
            try:
                self.handle(client)
            except Exception, e:
                self.log(client, 'exception: %s' % e)
                break
        self.handle_quit(client)
        self.log(client, 'exiting handler')


    def rotate(self):
        """Log rotation"""
        self.log_file.close()
        self.log_file = open(self.log_name, 'w')


    def run(self):
        """Main loop for standard socket handling"""
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
                    self.log(client, 'client connected')
                    self.select(client, 0)
                else:
                    try:
                        self.handle(self.clients[sock])
                    except Exception, e:
                        self.log(client, 'exception: %s' % e)
                        self.handle_quit(client)
        for client_socket in self.clients.iterkeys():
            client_socket.close()
        self.clients.clear()
        server.close()


    def run_gevent(self):
        """Main loop for gevent handling"""
        server = gevent.server.StreamServer((self.host, self.port), self.gevent_handler)
        server.serve_forever()


    def save(self):
        """Serialize tables to disk"""
        self.meta['expiries'] = self.expiries
        for db in self.tables:
            self.meta[db] = self.tables[db]
        self.meta.commit()
        self.lastsave = int(time.time())


    def select(self, client, db):
        if db not in self.tables:
            self.tables[db] = self.meta.get(db,{})
        client.db = db
        client.table = self.tables[db]


    def stop(self):
        if not self.halt:
            self.log(None, 'STOPPING')
            self.save()
            self.halt = True


    def handle_bgsave(self, client):
        if hasattr(os, 'fork'):
            if not os.fork():
                self.save()
                sys.exit(0)
        else:
            self.save()
        self.log(client, 'BGSAVE')
        return RedisMessage('Background saving started')


    def handle_decr(self, client, key):
        self.check_ttl(client, key)
        return self.handle_decrby(self, client, key, 1)


    def handle_decrby(self, client, key, by):
        self.check_ttl(client, key)
        return self.handle_incrby(self, client, key, -by)


    def handle_del(self, client, key):
        self.handle_persist(client, key)
        self.log(client, 'DEL %s' % key)
        if key not in client.table:
            return 0
        del client.table[key]
        return 1


    def handle_persist(self, client, key):
        try:
            del self.expiries["%s %s" % (client.db,key)]
        except:
            pass


    def handle_expire(self, client, key, ttl):
        self.expiries["%s %s" % (client.db,key)] = time.time() + ttl
        return 1


    def handle_expireat(self, client, key, when):
        self.expiries["%s %s" % (client.db,key)] = when
        return 1


    def handle_ttl(self, client, key):
        if key not in client.table:
            return -2
        k = "%s %s" % (client.db, key)
        if k not in self.expiries:
            return -1
        return int(self.expiries[k])


    def handle_flushdb(self, client):
        self.log(client, 'FLUSHDB')
        client.table.clear()
        return True


    def handle_flushall(self, client):
        self.log(client, 'FLUSHALL')
        for table in self.tables.itervalues():
            table.clear()
        return True


    def handle_get(self, client, key):
        self.check_ttl(client, key)
        data = client.table.get(key, None)
        if isinstance(data, deque):
            return BAD_VALUE
        if data != None:
            data = str(data)
        else:
            data = EMPTY_SCALAR
        self.log(client, 'GET %s -> %d' % (key, len(data)))
        return data


    def handle_mget(self, client, keys):
        result = []
        for k in keys:
            self.check_ttl(client, k)
            data = client.table.get(k, None)
            if isinstance(data, deque):
                return BAD_VALUE
            if data != None:
                data = str(data)
            else:
                data = EMPTY_SCALAR
            result.append(data)
        self.log(client, 'MGET %s -> %s' % (keys, result))
        return result


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
        self.log(client, 'INCRBY %s %s -> %s' % (key, by, client.table[key]))
        return client.table[key]


    def handle_keys(self, client, pattern):
        r = re.compile('^' + pattern.replace('*', '.*') + '$') 
        self.log(client, 'KEYS %s' % pattern)
        return [k for k in client.table.keys() if r.match(k)]


    def handle_lastsave(self, client):
        return self.lastsave


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
        self.log(client, 'LPOP %s -> %s' % (key, data))
        return data


    def handle_lpush(self, client, key, data):
        self.check_ttl(client, key)
        if key not in client.table:
            client.table[key] = deque()
        elif not isinstance(client.table[key], deque):
            return BAD_VALUE
        client.table[key].appendleft(data)
        self.log(client, 'LPUSH %s %s' % (key, data))
        return True


    def handle_lrange(self, client, key, low, high):
        self.check_ttl(client, key)
        low, high = int(low), int(high)
        if low == 0 and high == -1:
            high = None
        if key not in client.table:
            return EMPTY_LIST
        if not isinstance(client.table[key], deque):
            return BAD_VALUE
        l = list(client.table[key])[low:high]
        self.log(client, 'LRANGE %s %s %s -> %s' % (key, low, high, l))
        return l


    def handle_ping(self, client):
        self.log(client, 'PING -> PONG')
        return RedisMessage('PONG')


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
        self.log(client, 'RPOP %s -> %s' % (key, data))
        return data


    def handle_rpush(self, client, key, data):
        self.check_ttl(client, key)
        if key not in client.table:
            client.table[key] = deque()
        elif not isinstance(client.table[key], deque):
            return BAD_VALUE
        client.table[key].append(data)
        self.log(client, 'RPUSH %s %s' % (key, data))
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
        self.log(client, 'QUIT')
        del self.clients[client.socket]
        return False


    def handle_save(self, client):
        self.save()
        self.log(client, 'SAVE')
        return True


    def handle_select(self, client, db):
        db = int(db)
        self.select(client, db)
        self.log(client, 'SELECT %s' % db)
        return True


    def handle_set(self, client, key, data):
        self.handle_persist(client, key)
        client.table[key] = data
        self.log(client, 'SET %s -> %d' % (key, len(data)))
        return True


    def handle_setnx(self, client, key, data):
        if key in client.table:
            self.log(client, 'SETNX %s -> %d FAILED' % (key, len(data)))
            return 0
        client.table[key] = data
        self.log(client, 'SETNX %s -> %d' % (key, len(data)))
        return 1
    

    def handle_getset(self, client, key, data):
        self.handle_persist(client, key)
        old_data = client.table.get(key, None)
        if isinstance(old_data, deque):
            return BAD_VALUE
        if old_data != None:
            old_data = str(old_data)
        else:
            old_data = EMPTY_SCALAR
        client.table[key] = data
        self.log(client, 'GETSET %s %s -> %s' % (key, data, old_data))
        return old_data


    def handle_rename(self, client, key, newkey):
        client.table[newkey] = client.table[key]
        k = "%s %s" % (client.db,key)
        # transfer TTL
        if k in self.expiries:
            self.expiries["%s %s" % (client.db,key)] = self.expiries[k]
            del self.expiries[k]
        del client.table[key]
        self.log(client, 'RENAME %s -> %s' % (key, newkey))
        return True


    def check_ttl(self, client, key):
        k = "%s %s" % (client.db,key)
        if k in self.expiries:
            if self.expiries[k] <= time.time():
                self.handle_del(client, key)


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
        self.log(client, 'SHUTDOWN')
        self.halt = True
        self.save()
        return self.handle_quit(client)


class ThreadedRedisServer(RedisServer):

    def __init__(self, **kwargs):
        super(ThreadedRedisServer, self).__init__(*kwargs)

    def thread(self, sock, address):
        client = RedisClient(sock)
        self.clients[sock] = client
        self.log(client, 'client connected')
        self.select(client,0)
        while True:
            try:
                self.handle(self.clients[sock])
            except Exception, e:
                self.log(client, 'exception: %s' % e)
                break
        try:
            self.handle_quit(client)
        except Exception, e:
            log.debug(">>> %s" % e)
            pass
        

def main(args):
    if os.name == 'posix':
        def sigterm(signum, frame):
            m.stop()
        def sighup(signum, frame):
            m.rotate()
        signal.signal(signal.SIGTERM, sigterm)
        signal.signal(signal.SIGHUP, sighup)

    host, port, log_file, db_file = '127.0.0.1', 6379, None, None
    opts, args = getopt.getopt(args, 'h:p:d:l:f:')
    pid_file = None
    for o, a in opts:
        if o == '-h':
            host = a
        elif o == '-p':
            port = int(a)
        elif o == '-l':
            log_file = os.path.abspath(a)
        elif o == '-d':
            db_file = os.path.abspath(a)
        elif o == '-f':
            pid_file = os.path.abspath(a)
    if pid_file:
        with open(pid_file, 'w') as f:
            f.write('%s\n' % os.getpid())

    m = RedisServer(host=host, port=port, log_file=log_file, db_file=db_file)
    try:
        m.run()
    except KeyboardInterrupt:
        m.stop()
    if pid_file:
        os.unlink(pid_file)
    sys.exit(0)


if __name__ == '__main__':
    main(sys.argv[1:])
