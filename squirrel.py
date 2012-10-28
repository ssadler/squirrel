import logging as _logging
import weakref
import psycopg2
import psycopg2.extensions
from collections import deque
from functools import partial
from tornado import ioloop, stack_context, gen


logger = _logging.getLogger('squirrel.pool')


class ConnectionPool(object):
    """
    Manages connections and provisions cursors.
    """
    def __init__(self, io_loop, max_connections=10, **connect_kwargs):
        self.io_loop = io_loop
        self.max_connections = max_connections
        connect_kwargs['async'] = 1
        self.connect_kwargs = connect_kwargs
        self.connections = deque()
        self.queue = deque()
        self.owed = 0
        self._refs = set()
        self._closed = False

    def cursor(self, callback):
        """ Get a cursor """
        dispatch = partial(self._dispatch, callback)
        dispatch = stack_context.wrap(dispatch)
        self.queue.append(dispatch)
        self._process_queue()

    def execute(self, sql, args, callback):
        """ Shortcut to execute a query """
        self.cursor(lambda cursor: cursor.execute(sql, args, callback))

    def close(self):
        """
        Closes the pool. The effect of this is not that the pool can no
        longer be used, but that connections will be closed if not in use.
        """
        self._closed = True
        while self.connections:
            self.connections.pop().close()

    def _process_queue(self):
        with stack_context.NullContext():
            while self.queue and self.owed < self.max_connections:
                self.owed += 1
                self.queue.popleft()()

    def _checkin(self, connection, err=None):
        """ Called automatically on dereference of CursorFairy """
        self.owed -= 1
        if err:
            logger.error("Error: %s, closing connection" % err)
            connection.close()
        elif self._closed:
            connection.close()
        elif not connection.closed:
            self.connections.append(connection)

    @gen.engine
    def _dispatch(self, callback):
        try:
            connection = self.connections.popleft()
        except IndexError:
            connection = psycopg2.connect(**self.connect_kwargs)
        try:
            yield gen.Task(poll, connection, self.io_loop)
        except Exception as e:
            self._checkin(connection, e)
            raise
        else:
            fairy = self._make_fairy(connection.cursor())
            callback(fairy)

    def _make_fairy(self, cursor):
        # We must be careful here not to make a reference to the fairy,
        # or it will never be dereferenced. But, we must make a reference to
        # the cursor, or it will be dereferenced with the fairy.
        on_deref = partial(self._on_fairy_deref, cursor)
        fairy = CursorFairy(self.io_loop, cursor)
        ref = weakref.ref(fairy, on_deref)
        self._refs.add(ref)
        return fairy

    def _on_fairy_deref(self, cursor, ref):
        with stack_context.NullContext():
            self._refs.remove(ref)
            self._checkin(cursor.connection)
            self.io_loop.add_callback(self._process_queue)


class CursorFairy(object):
    CONNECTION_WARN = False

    def __init__(self, io_loop, cursor):
        self._io_loop = io_loop
        self._cursor = cursor

    def __getattr__(self, name):
        """ Proxy missing attribute lookups to the cursor """
        return getattr(self._cursor, name)

    @property
    def connection(self):
        if not self.CONNECTION_WARN:
            self.CONNECTION_WARN = True
            logger.warning("Using the connection directly may cause "
                           "inconsistent state of the poller!")
        return self._cursor.connection

    def execute(self, sql, args, callback):
        self._cursor.execute(sql, args)
        self.poll(callback)

    def poll(self, callback):
        # bind self as first argument of callback.
        # this makes the cursor available to the
        # callee and ensures we aren't dereferenced until
        # the query has finished executing.
        callback = partial(callback, self)
        poll(self._cursor.connection, self._io_loop, callback)


class poll(object):
    """
    A poller that polls the PostgreSQL connection and calls the callback
    when the connection state is `POLL_OK`, or an error occurs.
    """
    def __init__(self, connection, io_loop, callback):
        self.connection = connection
        self.io_loop = io_loop
        self.callback = callback
        self.tick(connection.fileno(), 0)

    def tick(self, fd, events):
        mask = -1
        try:
            mask = STATE_MAP.get(self.connection.poll())
            if mask > 0:
                if events == 0:
                    self.io_loop.add_handler(fd, self.tick, mask)
                elif events > 0:
                    self.io_loop.update_handler(fd, mask)
            elif mask < 0:
                raise psycopg2.OperationalError("Connection has unknown error state")
        except:
            self.callback = None
            raise
        finally:
            if mask <= 0:
                if events:
                    self.io_loop.remove_handler(fd)
            if mask == 0:
                self.callback()

STATE_MAP = {
    psycopg2.extensions.POLL_OK: 0,
    psycopg2.extensions.POLL_READ: ioloop.IOLoop.ERROR | ioloop.IOLoop.READ,
    psycopg2.extensions.POLL_WRITE: ioloop.IOLoop.ERROR | ioloop.IOLoop.WRITE,
    psycopg2.extensions.POLL_ERROR: -1,
}
