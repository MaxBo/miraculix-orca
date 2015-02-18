#!/usr/bin/env python
#coding:utf-8

import psycopg2
from psycopg2.extras import NamedTupleConnection
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from types import MethodType

import logging
logger = logging.getLogger()


class Login(object):
    """
    Login-Object with the Database credentials
    """
    def __init__(self,
                 host='192.168.198.24',
                 port=5432,
                 user='postgres',
                 password='ggr',
                 db='bahn_db',
                 ):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.db = db

    def __repr__(self):
        """
        """
        msg = 'host={h}, port={p}, user={U}, password={pw}, db={db}'
        return msg.format(h=self.host, p=self.port, U=self.user,
                          pw=self.password, db=self.db)


class Connection(object):
    """
    Connection object
    """
    def __init__(self, login=None):
        if login is None:
            login = Login()
        self.login = login

    def __enter__(self):
        login = self.login
        conn = psycopg2.connect(host=login.host,
                              user=login.user,
                              #password=login.password,
                              port=login.port,
                              database=login.db,
                              connection_factory=NamedTupleConnection)
        self.conn = conn
        self.set_copy_command_format()
        self.set_vacuum_analyze_command()
        return conn

    def __exit__(self, t, value, traceback):
        self.conn.close()

    def set_copy_command_format(self):
        """
        sets the csv-format
        """
        data_format = 'CSV'
        quote = '"'
        delimiter = ','
        strWith = '''WITH (
        FORMAT {data_format},
        DELIMITER '{delimiter}',
        QUOTE '"',
        HEADER);
        '''.format(data_format=data_format, quote=quote,
                                      delimiter=delimiter)
        self.conn.copy_sql = '''COPY "{tn}" TO STDOUT ''' + strWith

    def set_vacuum_analyze_command(self):
        """
        run vacuum analze on a table with the according transaction isolation
        """
        def vacuum_analyze(self, table):
            old_isolation_level = self.isolation_level
            self.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cursor = self.cursor()
            cursor.execute('VACUUM ANALYZE {table};'.format(table=table))
            self.set_isolation_level(old_isolation_level)

        self.conn.vacuum_analyze = MethodType(vacuum_analyze,
                                              self.conn,
                                              self.conn.__class__)


class DBApp(object):
    """

    """
    def __init__(self, schema='osm'):
        """
        """
        self.schema = schema

    def set_db_user(self, login):
        """
        set login user
        """
        self.login = login

    def run_query(self, sql, conn=None, values=None, many=False):
        """
        runs an sql query in seperate steps and
        log the statusmessage of each query

        Parameters
        ----------
        sql : str
            the queries in a string, separated by ;
        conn : Connection-Instance (optional)
            if not given, than the default connection self.conn is taken
        values : tuple or list or list of tuples or array
            if given, the values are passed to the query
        many : bool, Default=False
            if True, pass vars to cur.executemany instead of cur.execute
        """
        if conn is None:
            conn = self.conn
        cur = conn.cursor()
        for query in sql.split(';'):
            if query.strip():
                logger.info(query)
                if many:
                    cur.executemany(sql, values)
                else:
                    cur.execute(query, values)
                logger.info(cur.statusmessage)

    def set_search_path(self, connstr='conn'):
        conn = getattr(self, connstr)
        sql = 'SET search_path TO %s, "$user", public;' % self.schema
        cur = conn.cursor()
        cur.execute(sql)

    def show_search_path(self):
        cur = self.conn.cursor()
        sql = 'show search_path ;'
        cur.execute(sql)
        rows = cur.fetchall()
        print rows