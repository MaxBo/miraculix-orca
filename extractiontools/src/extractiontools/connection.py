#!/usr/bin/env python
# coding:utf-8

import sys
import psycopg2
from psycopg2.extras import NamedTupleConnection, DictCursor
from collections import OrderedDict
import sqlparse
from psycopg2.sql import SQL, Composed, Literal
from copy import deepcopy
from typing import Union, Dict

import os
import logging


class Login:
    """
    Login-Object with the Database credentials
    """

    def __init__(self,
                 host='localhost',
                 port=5432,
                 user='postgres',
                 password='',
                 db='',
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


class Connection:
    """
    Connection object
    """

    def __init__(self, login=None):
        self.login = login or Login()

    def __enter__(self) -> NamedTupleConnection:
        login = self.login
        conn = psycopg2.connect(host=login.host,
                                user=login.user,
                                password=login.password,
                                port=login.port,
                                database=login.db,
                                connection_factory=NamedTupleConnection,
                                sslmode='prefer')
        self.conn = conn
        self.conn.get_dict_cursor = self.get_dict_cursor
        self.conn.get_column_dict = self.get_column_dict
        self.set_copy_command_format()
        return conn

    def __exit__(self, t, value, traceback):
        self.conn.commit()
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

    def get_dict_cursor(self):
        return self.conn.cursor(cursor_factory=DictCursor)

    def get_columns(self, tablename):
        """
        Return a tuple of columns of a table

        Parameters
        ----------
        tablename : str
            the tablename or schema.tablename to query

        Returns
        -------
            tuple of Column objects
        """
        cur = self.get_dict_cursor()
        sql = 'SELECT * FROM {} LIMIT 0;'.format(tablename)
        cur.execute(sql)
        descr = cur.description
        return descr

    def get_column_dict(self, tablename, schema=None):
        """
        Return a tuple of column names of a table

        Parameters
        ----------
        tablename : str
            the tablename or schema.tablename to query

        schema : str, optional
            the schemaname

        Returns
        -------
        cols : Ordered Dict of the columns
        """
        if schema is not None:
            table = '{s}.{t}'.format(s=schema, t=tablename)
        else:
            table = tablename
        descr = self.get_columns(table)
        return OrderedDict(((d.name, d) for d in descr))


def get_foreign_login(database):
    return Login(
        host=os.environ.get('FOREIGN_HOST', 'localhost'),
        port=os.environ.get('FOREIGN_PORT', 5432),
        user=os.environ.get('FOREIGN_USER', 'osm'),
        password=os.environ.get('FOREIGN_PASS', ''),
        db=database
    )


class DBApp:
    """

    """
    role = None

    def __init__(self, schema='osm', conn=None, logger=None):
        """
        """
        self.schema = schema
        self.logger = logger or logging.getLogger(self.__module__)
        self.conn = conn

    def set_login(self, host: str = None, port: int = None, user: str = None,
                  password: str = None, database: str = None):
        self.login = Login(
            host or os.environ.get('DB_HOST', 'localhost'),
            port or os.environ.get('DB_PORT', 5432),
            user or os.environ.get('DB_USER', 'postgres'),
            password or os.environ.get('DB_PASS', ''),
            database
        )

    def create_schema(self, schema, conn=None, replace=False):
        if replace:
            sql = f'DROP SCHEMA IF EXISTS {schema} CASCADE;'
            self.run_query(sql, conn=conn or self.conn)
        sql = f'CREATE SCHEMA IF NOT EXISTS {schema};'
        self.run_query(sql, conn=conn or self.conn)

    def check_platform(self):
        """
        check the platform
        """
        if sys.platform.startswith('win'):
            self.folder = r'C:\temp'
            self.SHELL = False
        else:
            self.folder = '/root/gis'
            self.SHELL = True

    def make_folder(self, folder: str):
        """
        create the subfolder `folder`, if it does not exist
        raise an IOError, if this fails
        """
        self.check_platform()
        if not os.path.exists(folder):
            os.makedirs(folder)

    def run_query(self, sql: Union[str, Composed], conn=None,
                  split=True, verbose=True, vars: Dict[str, object] = None):
        """
        runs an sql query log the statusmessage of each query

        Parameters
        ----------
        sql : str or SQL
            the queries in a string, separated by ; or
            formatted composed SQL
        conn : Connection-Instance (optional)
            if not given, than the default connection self.conn is taken
        split : bool (optional)
            run the sql query in seperate steps, defaults to True
        verbose: bool(optional, default=True)
            if true, log query
        vars: dict(optional)
            values to pass to the query
        """
        conn = conn or self.conn
        cur = conn.cursor()

        def execute(query, vars=None):
            if verbose:
                self.logger.info(query)
            else:
                self.logger.debug(query)
            cur.execute(query, vars)

        if split:
            query_string = sql.as_string(
                conn) if isinstance(sql, Composed) else sql
            for query in sqlparse.split(query_string):
                if query.strip().rstrip(';'):
                    query_without_comments = '\n'.join([
                        q for q in query.replace('\r', '').split('\n')
                        if not q.strip().startswith('--')])
                    if query_without_comments.strip():
                        execute(query, vars)
        else:
            execute(sql, vars)

    def set_search_path(self, connstr: str = 'conn'):
        conn = getattr(self, connstr)
        sql = f'SET search_path TO {self.schema}, "$user", public;'
        cur = conn.cursor()
        cur.execute(sql)

    def show_search_path(self):
        cur = self.conn.cursor()
        sql = 'show search_path ;'
        cur.execute(sql)
        rows = cur.fetchall()
        self.logger.info(rows)

    def set_session_authorization(self, conn):
        """
        Set session authorization to self.role, if exists
        """
        # if a role is defined, create this schema with this role and also
        # perform all queries during this transaction with this role
        if self.role:
            sql = "SET SESSION SESSION AUTHORIZATION '{role}';"
            self.run_query(sql.format(role=self.role), conn)

    def reset_authorization(self, conn):
        """Reset role to Login Role"""
        sql = """RESET SESSION AUTHORIZATION;"""
        self.run_query(sql, conn)

    def check_if_database_exists(self, db_name: str):
        """
        checks if database exists

        Parameters
        ----------
        db_name : str
            the database to search

        Returns
        -------
        exists : bool
        """
        login = deepcopy(self.login)
        login.db = 'postgres'
        with Connection(login=login) as conn:
            cursor = conn.cursor()
            sql = SQL('SELECT 1 AS e FROM pg_database WHERE datname = {}'
                      ).format(Literal(db_name))
            cursor.execute(sql)
            rows = cursor.fetchall()
        return len(rows) > 0

    def drop_database(self, dbname: str, conn=None):
        """
        Drop database, disconnect all connections before

        Parameters
        ----------
        dbname : str
            the name of the database to drop
        conn : Connection-instance, optional
            the connection to use to execute the drop commands
        """
        if conn is None:
            conn = self.conn
        sql = """
update pg_database set datallowconn = 'false' where datname = '{db}';
SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '{db}';
            """.format(db=dbname)
        self.run_query(sql, conn)

        cur = conn.cursor()
        sql = """
DROP DATABASE IF EXISTS {db};
        """
        conn.set_isolation_level(0)
        cur.execute(sql.format(db=dbname))
        conn.set_isolation_level(1)
        conn.commit()

    def add_raster_index(self, schema, tablename, raster_column='rast', conn=None):
        """
        add a raster index
        """
        if schema is None:
            raise ValueError('please define schema')
        conn = conn or self.conn
        sql = """
CREATE INDEX idx_{tn}_geom ON {schema}.{tn} USING gist(st_convexhull({rast}));
SELECT AddRasterConstraints('{schema}', '{tn}', '{rast}', TRUE, TRUE, TRUE, TRUE,
                            TRUE, TRUE, TRUE, TRUE, TRUE, TRUE, TRUE, TRUE);
        """
        self.run_query(sql.format(schema=schema, tn=tablename, rast=raster_column),
                       conn=conn)
        conn.commit()

    def add_overview_index(self, overviews, schema, tablename, raster_column='rast', conn=None):
        """
        Add an index to all given overview rasters for the given raster table
        """
        conn = conn or self.conn
        for ov in overviews:
            ov_tn = 'o_{ov}_{tn}'.format(ov=ov, tn=tablename)
            sql = '''
SELECT AddOverviewConstraints('{schema}', '{ov_tn}', '{rast}',
                              '{schema}', '{tn}', '{rast}', {ov});

            '''
            self.run_query(sql.format(schema=schema,
                                      tn=tablename,
                                      ov_tn=ov_tn,
                                      ov=ov,
                                      rast=raster_column), conn=conn)
            self.add_raster_index(schema, ov_tn, raster_column=raster_column)
        conn.commit()

    def add_raster_index_and_overviews(self, overviews, schema, tablename,
                                       raster_column='rast', conn=None):
        conn = conn or self.conn
        self.add_raster_index(schema, tablename, raster_column, conn)
        self.add_overview_index(
            overviews, schema, tablename, raster_column, conn)

    def get_primary_key(self, schema, tablename, conn=None):
        """
        Return the primary key columns of schema.tablename as string
        """
        conn = conn or self.conn
        sql = """
SELECT a.attname
FROM   pg_index i
JOIN   pg_attribute a ON a.attrelid = i.indrelid
                     AND a.attnum = ANY(i.indkey)
WHERE  i.indrelid = '"{s}"."{t}"'::regclass
AND    i.indisprimary;
        """.format(s=schema, t=tablename)
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        pkey = ', '.join(['"{}"'.format(r[0]) for r in rows])
        return pkey
