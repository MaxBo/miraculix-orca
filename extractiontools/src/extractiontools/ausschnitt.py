#!/usr/bin/env python
# coding:utf-8

from argparse import ArgumentParser

import numpy as np
import logging
logger = logging.getLogger('OrcaLog')
logger.level = logging.DEBUG
import sys
import os
import subprocess
from copy import deepcopy
from psycopg2.sql import Identifier, Literal, SQL
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from .connection import Connection, DBApp, Login


class BBox(object):
    """
    Represents a Bounding Box
    """

    def __init__(self, top, bottom, left, right):
        """
        Parameters
        ----------
        top, bottom, left, right; float
        """
        self.top = top
        self.bottom = bottom
        self.left = left
        self.right = right

    def __repr__(self):
        """a nice representation of the bounding box"""
        msg = '''A bounding box with the coordinates:
top: {t:0.2f}, bottom: {b:0.2f}, left: {l: 0.2f}, right: {r:0.2f}'''
        return msg.format(t=self.top, b=self.bottom,
                          l=self.left, r=self.right)

    def rounded(self, digits=1):
        """return the bbox as rounded values"""
        lon0 = round(self.left, digits)
        lon1 = round(self.right, digits)
        lat0 = round(self.bottom, digits)
        lat1 = round(self.top, digits)
        return lon0, lon1, lat0, lat1


class Extract(DBApp):
    """
    extracts data from an existing database into a new database
    """
    tables = {}
    role = 'group_osm'

    def __init__(self,
                 destination_db: str='extract',
                 target_srid: int=31467,
                 temp: str='temp',
                 foreign_server: str='foreign_server',
                 login: Login=None,
                 foreign_login: Login=None,
                 **options):
        self.srid = 4326
        self.temp = temp
        self.foreign_server = foreign_server
        self.target_srid = target_srid
        self.source_db = options.get('source_db', 'europe')
        self.destination_db = destination_db
        self.tables2cluster = []
        self.check_platform()
        if login:
            self.login = login
        else:
            self.set_login(
                os.environ.get('DB_HOST', 'localhost'),
                os.environ.get('DB_PORT', 5432),
                os.environ.get('DB_USER'),
                password=os.environ.get('DB_PASS', '')
            )
        self.foreign_login = foreign_login or Login(
            host=os.environ.get('FOREIGN_HOST', 'localhost'),
            port=os.environ.get('FOREIGN_PORT', 5432),
            user=os.environ.get('FOREIGN_USER'),
            password=os.environ.get('FOREIGN_PASS', ''),
            db=os.environ.get('FOREIGN_NAME', 'europe')
        )

    def set_login(self, host, port, user, password=None, **kwargs):
        """
        set login information for destination database

        Parameters
        ----------
        host : str
        port : int
        user : str
        password : str, optional
        """
        self.login = Login(host, port, user, password, db=self.destination_db)

    def recreate_db(self):
        self.set_pg_path()
        exists = self.check_if_database_exists(self.destination_db)
        if exists:
            logger.info(f'Truncate Database {self.destination_db}')
            self.truncate_db()
        else:
            logger.info(f'Create Database {self.destination_db}')
            self.create_target_db(self.login)
            logger.info(f'Create Database {self.destination_db}')
        self.create_extensions()
        self.create_foreign_server()
        self.create_serverside_folder()

    def extract(self):
        with Connection(login=self.login) as conn:
            self.conn = conn
            self.create_foreign_schema()
            self.create_target_boundary()
            #for tn, geom in self.tables.items():
                #self.extract_table(tn, geom)
            self.additional_stuff()
            self.conn.commit()
            self.rename_schema()
            self.final_stuff()
            #self.cleanup()
            self.conn.commit()

    def further_stuff(self):
        """
        To be defined in the subclass
        """

    def additional_stuff(self):
        """
        additional steps, to be defined in the subclass
        """

    def create_foreign_server(self):
        sql = f"""
        -- server
        DROP SERVER IF EXISTS {self.foreign_server} CASCADE;
        CREATE SERVER {self.foreign_server}
        FOREIGN DATA WRAPPER postgres_fdw
        OPTIONS (host '{self.foreign_login.host}',
        port '{self.foreign_login.port}', dbname '{self.foreign_login.db}');
        -- user
        CREATE USER MAPPING FOR {self.login.user}
        SERVER {self.foreign_server}
        OPTIONS (user '{self.foreign_login.user}',
        password '{self.foreign_login.password}');
        """
        logger.info(
            f'Creating connection to database "{self.foreign_login.db}"')
        with Connection(login=self.login) as conn:
            self.run_query(sql, conn=conn)
            conn.commit()

    def create_extensions(self):
        """
        extensions needed later (usually provided by the template already)
        """
        sql = '''
        CREATE EXTENSION IF NOT EXISTS dblink;
        CREATE EXTENSION IF NOT EXISTS postgis;
        CREATE EXTENSION IF NOT EXISTS postgis_raster;
        CREATE EXTENSION IF NOT EXISTS hstore;
        CREATE EXTENSION IF NOT EXISTS pgRouting;
        CREATE EXTENSION IF NOT EXISTS kmeans;
        CREATE EXTENSION IF NOT EXISTS plpython3u;
        CREATE EXTENSION IF NOT EXISTS postgres_fdw;
        CREATE OR REPLACE AGGREGATE public.hstore_sum (public.hstore)
        (
          SFUNC = public.hs_concat,
          STYPE = public.hstore
        );
        '''
        logger.info('Adding extensions')
        with Connection(login=self.login) as conn:
            self.run_query(sql, conn=conn)
            conn.commit()

    def rename_schema(self):
        """
        """
        # Rename restored temp-Schema to new name
        sql = '''
ALTER SCHEMA {temp} RENAME TO {schema}
        '''.format(temp=self.temp,
                   schema=self.schema)
        self.run_query(sql, self.conn1)

    def final_stuff(self):
        """
        CREATE INDEX in the target DB
        """
        self.create_index()

    def create_index(self):
        """
        To be defined in the subclass
        """

    def extract_table(self, tn, geom='geom'):
        """
        extracts a single table
        """
        geometrytype = self.get_geometrytype(tn, geom)
        cols = self.conn0.get_column_dict(tn, self.schema)
        cols_without_geom = ('t."{}"'.format(c) for c in cols if c != geom)
        col_str = ', '.join(cols_without_geom)

        sql = """
SELECT {cols}, st_transform(t.{geom}, {srid})::geometry({gt}, {srid}) as geom
INTO {temp}.{tn}
FROM {schema}.{tn} t, {temp}.boundary tb
WHERE
st_intersects(t.{geom}, tb.source_geom)
        """
        self.run_query(sql.format(tn=tn, temp=self.temp, geom=geom,
                                  schema=self.schema, cols=col_str, srid=self.target_srid,
                                  gt=geometrytype),
                       conn=self.conn0)

    def get_geometrytype(self, tn, geom):
        sql = """
SELECT geometrytype({geom}) FROM {sn}.{tn} LIMIT 1;
        """.format(geom=geom, sn=self.schema, tn=tn)
        cur = self.conn0.cursor()
        cur.execute(sql)
        geometrytype = cur.fetchone()[0]
        return geometrytype

    def create_foreign_schema(self):
        """
        links schema in database to schema on foreign server
        """
        #sql = '''
#DROP SCHEMA IF EXISTS {temp} CASCADE;
#CREATE SCHEMA {temp};
        #'''.format(temp=self.temp)
        sql = f"""
        DROP SCHEMA IF EXISTS {self.temp} CASCADE;
        """
        self.run_query(sql, conn=self.conn)

        sql = f"""
        IMPORT FOREIGN SCHEMA {self.schema}
        FROM SERVER {self.foreign_server} INTO {self.temp};
        """

        self.run_query(sql, self.conn)

    def get_target_boundary(self, bbox):
        """
        """
        self.bbox = bbox

    def get_target_boundary_from_dest_db(self):
        """
        get the target boundary from the destination database
        """
        with Connection(login=self.login1) as conn1:
            cur = conn1.cursor()
            sql = """
SELECT
    st_ymax(a.source_geom) AS top,
    st_ymin(a.source_geom) AS bottom,
    st_xmax(a.source_geom) AS right,
    st_xmin(a.source_geom) AS left
FROM meta.boundary a;
"""
            cur.execute(sql)
            row = cur.fetchone()
            self.bbox = BBox(row.top, row.bottom, row.left, row.right)

        self.target_srid = self.get_target_srid_from_dest_db()

    def get_target_srid_from_dest_db(self):
        """
        get the target boundary from the destination database

        Returns
        -------
        srid : int
        """
        with Connection(login=self.login1) as conn1:
            cur = conn1.cursor()
            sql = """
SELECT
    st_srid(a.geom) AS srid
FROM meta.boundary a;
"""
            cur.execute(sql)
            row = cur.fetchone()
            srid = row.srid
        return srid

    def create_target_boundary(self):
        """
        write target boundary into temp_schema

        Parameters
        ----------
        top : float
        bottom : float
        left : float
        right : float
        """
        bbox = self.bbox

        sql = SQL('''
DROP TABLE IF EXISTS {temp}.boundary;
CREATE TABLE {temp}.boundary (id INTEGER PRIMARY KEY,
                              source_geom geometry('POLYGON', {srid}),
                              geom geometry('POLYGON', {target_srid}));
INSERT INTO {temp}.boundary (id, source_geom)
VALUES (1, st_setsrid(st_makebox2d(st_point({LEFT}, {TOP}), st_point({RIGHT}, {BOTTOM})), {srid}));
UPDATE {temp}.boundary SET geom = st_transform(source_geom, {target_srid});
''').format(temp=Identifier(self.temp),
            LEFT=Literal(bbox.left), RIGHT=Literal(bbox.right),
            TOP=Literal(bbox.top), BOTTOM=Literal(bbox.bottom),
            srid=Literal(self.srid),
            target_srid=Literal(self.target_srid))
        self.run_query(sql, self.conn)

    def set_pg_path(self):
        """"""
        pg_path = os.environ.get('PGPATH')
        if sys.platform.startswith('win'):
            self.PGPATH = pg_path or r'C:\Program Files\PostgreSQL\9.3\bin'
            self.SHELL = False
        else:
            self.PGPATH = pg_path or '/usr/bin'
            self.SHELL = True

    def create_target_db(self, login):
        """
        create the target database
        """
        sql = SQL("""
        CREATE DATABASE {db};
        ALTER DATABASE {db} OWNER TO {role};
        """).format(db=Identifier(self.login.db), role=Literal(self.role))

        login = deepcopy(self.login)
        login.db = 'postgres'
        with Connection(login=login) as conn:
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            self.run_query(sql, conn=conn)

    def truncate_db(self):
        """Truncate the database"""
        #sql = """
#SELECT pg_cancel_backend(pid) FROM pg_stat_activity WHERE datname = '{db}';
        #""".format(db=self.destination_db)
        #with Connection(login=self.login) as conn:
            #self.run_query(sql, conn=conn)
            #conn.commit()

        sql = """
SELECT 'drop schema if exists "' || schema_name || '" cascade;' AS sql
FROM (SELECT catalog_name, schema_name
      FROM information_schema.schemata
      WHERE schema_name not like 'pg_%' AND
      schema_name not IN ('information_schema', 'public',
                          'topology', 'repack')) s;
        """
        with Connection(login=self.login) as conn:
            cursor2 = conn.cursor()
            cursor = conn.cursor()
            cursor.execute(sql)
            for row in cursor:
                cursor2.execute(row.sql)
            conn.commit()

        #sql = """
#update pg_database set datallowconn = 'True' where datname = '{db}';
        #""".format(db=self.destination_db)
        #with Connection(login=self.login0) as conn:
            #self.run_query(sql, conn=conn)
            #conn.commit()

    def cleanup(self):
        """
        remove the temp schema
        """
        with Connection(login=self.login) as conn0:
            sql = SQL('''DROP SCHEMA IF EXISTS {schema} CASCADE''').format(
                schema=Identifier(self.temp))
            self.run_query(sql, conn=conn0)

    def cluster_and_analyse(self):
        """
        """
        login = self.login1

        cmd = '''"{clusterdb}" -U {user} -h {host} -p {port} -w --verbose {tbls} -d {destination_db}'''
        if self.tables2cluster:
            tbls = ' '.join(('-t {}'.format(d) for d in self.tables2cluster))
            clusterdb = os.path.join(self.PGPATH, 'clusterdb')
            cmd = cmd.format(clusterdb=clusterdb,
                             destination_db=login.db,
                             user=login.user,
                             port=login.port,
                             host=login.host,
                             tbls=tbls)
            logger.info(cmd)
            subprocess.call(cmd, shell=self.SHELL)
        else:
            logger.info('no tables to cluster')

        vacuumdb = os.path.join(self.PGPATH, 'vacuumdb')
        cmd = '''"{vacuumdb}" -U {user} -h {host} -p {port} -w --analyze-in-stages -d {destination_db}'''
        cmd = cmd.format(vacuumdb=vacuumdb,
                         destination_db=login.db,
                         user=login.user,
                         port=login.port,
                         host=login.host)
        logger.info(cmd)
        subprocess.call(cmd, shell=self.SHELL)

    def create_serverside_folder(self):
        """ Create a project folder on the server"""
        self.check_platform()
        folder = os.path.join(self.folder, 'projekte', self.destination_db)
        self.make_folder(folder)


if __name__ == '__main__':

    parser = ArgumentParser(description="Extract Data for Model")

    parser.add_argument("-n", '--name', action="store",
                        help="Name of destination database",
                        dest="destination_db", default='extract')

    parser.add_argument("-s", '--srid', action="store",
                        help="srid of the target database", type=int,
                        dest="srid", default='31467')

    parser.add_argument("-t", '--top', action="store",
                        help="top", type=float,
                        dest="top", default=54.65)
    parser.add_argument("-b", '--bottom,', action="store",
                        help="bottom", type=float,
                        dest="bottom", default=54.6)
    parser.add_argument("-r", '--right', action="store",
                        help="right", type=float,
                        dest="right", default=10.0)
    parser.add_argument("-l", '--left', action="store",
                        help="left", type=float,
                        dest="left", default=9.95)

    parser.add_argument('--host', action="store",
                        help="host",
                        dest="host", default='localhost')
    parser.add_argument("-p", '--port', action="store",
                        help="port", type=int,
                        dest="port", default=5432)

    parser.add_argument('--recreate', action="store_true",
                        help="recreate",
                        dest="recreate", default=False)

    parser.add_argument("-U", '--user', action="store",
                        help="database user",
                        dest="user", default='osm')
    parser.add_argument('--source-db', action="store",
                        help="source database",
                        dest="source_db", default='europe')

    options = parser.parse_args()

    bbox = BBox(top=options.top, bottom=options.bottom,
                left=options.left, right=options.right)
    extract = ExtractMeta(source_db=options.source_db,
                          destination_db=options.destination_db,
                          target_srid=options.srid)
    extract.set_login(host=options.host,
                      port=options.port, user=options.user)
    extract.get_target_boundary(bbox)
    extract.extract()

