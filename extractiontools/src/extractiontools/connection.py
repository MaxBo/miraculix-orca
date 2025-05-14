#!/usr/bin/env python
# coding:utf-8

import sys
import psycopg2
from psycopg2.extras import NamedTupleConnection, DictCursor
import sqlparse
from psycopg2.sql import SQL, Composed, Literal
from psycopg2.extensions import Column
from copy import deepcopy
from typing import Union, Dict, OrderedDict, Tuple, List
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from psycopg2.sql import Identifier, SQL

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
        self.conn.get_colums = self.get_columns
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
        strWith = f'''WITH (
        FORMAT {data_format},
        DELIMITER '{delimiter}',
        QUOTE '{quote}',
        HEADER);
        '''
        self.conn.copy_sql = '''COPY "{tn}" TO STDOUT ''' + strWith

    def get_dict_cursor(self):
        return self.conn.cursor(cursor_factory=DictCursor)

    def get_columns(self, tablename: str) -> Tuple[Column, ...]:
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
        sql = f'SELECT * FROM {tablename} LIMIT 0;'
        cur.execute(sql)
        descr = cur.description
        return descr

    def get_column_dict(self,
                        tablename: str,
                        schema: str = None) -> OrderedDict[str, Column]:
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
            table = f'"{schema}"."{tablename}"'
        else:
            table = f'"{tablename}"'
        descr = self.get_columns(table)
        return OrderedDict(((d.name, d) for d in descr))


class DBApp:
    """

    """
    role = None

    def __init__(self,
                 schema: str = 'osm',
                 conn: NamedTupleConnection = None,
                 logger: logging.Logger = None):
        """
        """
        self.schema = schema
        self.logger = logger or logging.getLogger(self.__module__)
        self.conn = conn

    def set_login(self,
                  host: str = None,
                  port: int = None,
                  user: str = None,
                  password: str = None,
                  database: str = None):
        self.login = Login(
            host or os.environ.get('DB_HOST', 'localhost'),
            port or os.environ.get('DB_PORT', 5432),
            user or os.environ.get('DB_USER', 'postgres'),
            password or os.environ.get('DB_PASS', ''),
            database
        )

    def create_target_db(self, database: str = None, role: str = None):
        """
        create the target database
        """

        db = database or self.login.db
        self.role = role or self.role
        sql = SQL("""
        CREATE DATABASE {db};
        ALTER DATABASE {db} OWNER TO {role};
        """).format(db=Identifier(db), role=Identifier(role))

        login = deepcopy(self.login)
        login.db = 'postgres'
        with Connection(login=login) as conn:
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            self.run_query(sql, conn=conn)

    def set_table_comment(self, comment: str, table_name, schema='public', conn=None):
        sql = f"COMMENT ON TABLE {schema}.{table_name} IS '{comment}';"
        self.run_query(sql, conn=conn or self.conn)

    def create_schema(self,
                      schema: str,
                      conn: NamedTupleConnection = None,
                      replace: bool = False):
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

    def validate_table_exists(self, table: str):
        """
        validate that the [schema.]table
        is a valid table/view in the database and that it exists
        """
        schema_table = table.split('.')
        cur = self.conn.cursor()
        if len(schema_table) == 1:
            sql = 'Select exists(select * from information_schema.tables where table_name=%s)'
        elif len(schema_table) == 2:
            sql = 'Select exists(select * from information_schema.tables where table_schema=%s AND table_name=%s)'
        else:
            raise ValueError(
                f'{table} is no valid for schema.table')
        cur.execute(sql, schema_table)
        if not cur.fetchone()[0]:
            self.conn.rollback()
            raise ValueError(f'{table} does not exist')

    def run_query(self,
                  sql: Union[str, Composed],
                  conn: NamedTupleConnection = None,
                  split: bool = True,
                  verbose: bool = True,
                  vars: Dict[str, object] = None):
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

        Returns
        -------
        cursor : the cursor used for database operations
        """
        conn = conn or self.conn
        cur = conn.cursor()

        def execute(query, vars=None):
            if verbose:
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

        return cur

    def set_search_path(self, connstr: str = 'conn'):
        conn = getattr(self, connstr)
        sql = f'SET search_path TO {self.schema}, "$user", public;'
        cur = conn.cursor()
        cur.execute(sql)

    def show_search_path(self):
        """send the postgresql-search path to the logger"""
        cur = self.conn.cursor()
        sql = 'show search_path ;'
        cur.execute(sql)
        rows = cur.fetchall()
        self.logger.info(rows)

    def set_session_authorization(self, conn: NamedTupleConnection):
        """
        Set session authorization to self.role, if exists
        """
        # if a role is defined, create this schema with this role and also
        # perform all queries during this transaction with this role
        if self.role:
            sql = f"SET SESSION SESSION AUTHORIZATION '{self.role}';"
            self.run_query(sql, conn)

    def reset_authorization(self, conn: NamedTupleConnection):
        """Reset role to Login Role"""
        sql = """RESET SESSION AUTHORIZATION;"""
        self.run_query(sql, conn)

    def check_if_database_exists(self, db_name: str) -> bool:
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

    def drop_database(self, dbname: str, conn: NamedTupleConnection = None):
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
        sql = f"""
UPDATE pg_database set datallowconn = 'false' where datname = '{dbname}';
SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '{dbname}';
            """
        self.run_query(sql, conn)

        cur = conn.cursor()
        sql = f"""
DROP DATABASE IF EXISTS "{dbname}";
        """
        conn.set_isolation_level(0)
        cur.execute(sql)
        conn.set_isolation_level(1)
        conn.commit()

    def add_raster_index(self,
                         schema: str,
                         tablename: str,
                         raster_column: str = 'rast',
                         conn: NamedTupleConnection = None):
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

    def add_overview_index(self,
                           overviews: List[int],
                           schema: str,
                           tablename: str,
                           raster_column: str = 'rast',
                           conn: NamedTupleConnection = None):
        """
        Add an index to all given overview rasters for the given raster table
        """
        conn = conn or self.conn
        for ov in overviews:
            ov_tn = f'o_{ov}_{tablename}'
            sql = f'''
SELECT AddOverviewConstraints('{schema}', '{ov_tn}', '{raster_column}',
                              '{schema}', '{tablename}', '{raster_column}', {ov});

            '''
            self.run_query(sql, conn=conn)
            self.add_raster_index(schema, ov_tn, raster_column=raster_column)
        conn.commit()

    def add_raster_index_and_overviews(self,
                                       overviews: List[int],
                                       schema: str,
                                       tablename: str,
                                       raster_column: str = 'rast',
                                       conn: NamedTupleConnection = None):
        conn = conn or self.conn
        self.add_raster_index(schema, tablename, raster_column, conn)
        self.add_overview_index(
            overviews, schema, tablename, raster_column, conn)

    def get_primary_key(self,
                        schema: str,
                        tablename: str,
                        conn: NamedTupleConnection = None) -> str:
        """
        Return the primary key columns of schema.tablename as string
        """
        conn = conn or self.conn
        sql = f"""
SELECT a.attname
FROM   pg_index i
JOIN   pg_attribute a ON a.attrelid = i.indrelid
                     AND a.attnum = ANY(i.indkey)
WHERE  i.indrelid = '"{schema}"."{tablename}"'::regclass
AND    i.indisprimary;
        """
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        pkey = ', '.join([f'"{r[0]}"' for r in rows])
        return pkey

    def get_columns(self, schema: str, tablename: str, conn: NamedTupleConnection = None):
        conn = conn or self.conn
        sql = f"""        
            SELECT column_name FROM information_schema.columns    
            WHERE table_schema = '{schema}' AND table_name = '{tablename}';
        """
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        return [r.column_name for r in rows]

