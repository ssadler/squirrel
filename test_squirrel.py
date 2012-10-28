import psycopg2
import itertools
from mock import patch
from squirrel import ConnectionPool
from tornado.testing import AsyncTestCase
from tornado import gen


class ConnectionPoolTestCase(AsyncTestCase):
    def async(self, func, *args, **kwargs):
        args = args + (self.stop,)
        func(*args, **kwargs)
        return self.wait()

    def setUp(self):
        super(ConnectionPoolTestCase, self).setUp()
        dsn = "host=127.0.0.1 dbname=test port=5432"
        self.provider = ConnectionPool(dsn=dsn,
                                      io_loop=self.io_loop)

    def test_connect_error_propagates_exception(self):
        provider = ConnectionPool(dsn="host=127.0.0.1 dbname=test port=6432",
                                      io_loop=self.io_loop)
        provider.cursor(self.stop)
        self.assertRaises(psycopg2.OperationalError, self.wait)

    def test_query(self):
        cursor = self.async(self.provider.cursor)
        self.async(cursor.execute, 'select 1', ())
        self.assertEqual((1,), cursor.fetchone())

    def test_query_error_propagates_error(self):
        cursor = self.async(self.provider.cursor)
        cursor.execute("I AM BAD", (), self.stop)
        self.assertRaises(psycopg2.ProgrammingError, self.wait)

    def test_query_error_dereferences(self):
        return
        try:
            self.async(self.provider.execute, "I AM BAD", ())
        except:
            import sys
            sys.exc_clear()
            self.assertEqual(1, len(self.provider.connections))

    def test_query_error_propagates_error_2(self):
        self.provider.execute("I AM BAD", (), self.stop)
        self.assertRaises(psycopg2.ProgrammingError, self.wait)

    @patch.object(ConnectionPool, '_checkin')
    def test_connection_checkin_on_deref(self, checkin):
        cursor = self.async(self.provider.cursor)
        connection = cursor.connection
        cursor = None
        checkin.assert_called_once_with(connection)

    @patch.object(ConnectionPool, '_checkin')
    def test_shorthand_execute_doesnt_deref_fairy(self, checkin):
        self.provider.cursor(self.stop)
        self.wait().execute("select 1", (), self.stop)
        self.assertEqual(0, checkin.call_count)
        self.wait()  # cursor returned here but we dont reference it
        self.assertEqual(1, checkin.call_count)

    def test_100_queries(self):
        n = 100
        c = itertools.count().next

        @gen.engine
        def query(i):
            if i % 2:
                cursor = yield gen.Task(self.provider.execute, 'select %s', (i,))
                self.assertEqual((i,), cursor.fetchone())
            else:
                try:
                    cursor = yield gen.Task(self.provider.cursor)
                    yield gen.Task(cursor.execute, 'select _%s' % i, ())
                except psycopg2.ProgrammingError as e:
                    self.assertIn('select _%s' % i, e.pgerror)

            if c() == n - 1:
                self.stop()

        for i in range(n):
            query(i)

        self.wait()

    def test_close_pool_eventually_closes_everything(self):
        cursor1 = self.async(self.provider.cursor)
        cursor2 = self.async(self.provider.cursor)
        conn2 = cursor2.connection
        cursor2 = None
        self.assertEqual(1, len(self.provider.connections))
        self.assertEqual(1, self.provider.owed)
        self.provider.close()
        self.assertTrue(conn2.closed)
        self.assertEqual(0, len(self.provider.connections))
        conn1 = cursor1.connection
        cursor1 = None
        self.assertEqual(0, self.provider.owed)
        self.assertTrue(conn1.closed)

