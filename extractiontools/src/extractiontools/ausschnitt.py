#!/usr/bin/env python
# coding:utf-8

from argparse import ArgumentParser

import sys
import os
import subprocess
import time
import ogr
from psycopg2.sql import Identifier, SQL, Literal
from psycopg2 import errors
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from copy import deepcopy
import logging

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
    foreign_schema = None
    schema = None

    def __init__(self,
                 destination_db,
                 target_srid: int=None,
                 temp: str=None,
                 foreign_server: str='foreign_server',
                 foreign_login: Login=None,
                 tables: dict={},
                 source_db: str=None,
                 logger=None,
                 boundary: ogr.Geometry=None,
                 boundary_name: str='bbox',
                 **options):
        self.srid = 4326
        self.logger = logger or logging.getLogger(__name__)
        self.temp = temp or f'temp{round(time.time() * 100)}'
        self.foreign_server = foreign_server
        self.source_db = source_db or os.environ.get('FOREIGN_NAME', 'europe')
        self.destination_db = destination_db
        self.tables = tables
        self.tables2cluster = []
        self.check_platform()
        self.boundary = boundary
        self.boundary_name = boundary_name
        self.set_login(database=self.destination_db)
        self.foreign_login = foreign_login or Login(
            host=os.environ.get('FOREIGN_HOST', 'localhost'),
            port=os.environ.get('FOREIGN_PORT', 5432),
            user=os.environ.get('FOREIGN_USER'),
            password=os.environ.get('FOREIGN_PASS', ''),
            db=self.source_db
        )
        self.target_srid = (target_srid or self.get_target_srid()
                            or 25832)

    def recreate_db(self):
        self.set_pg_path()
        exists = self.check_if_database_exists(self.destination_db)
        if exists:
            self.logger.info(f'Truncate Database {self.destination_db}')
            self.truncate_db()
        else:
            self.logger.info(f'Create Database {self.destination_db}')
            self.create_target_db(self.login)
            self.logger.info(f'Create Database {self.destination_db}')
        self.create_meta()
        self.create_extensions()
        self.create_foreign_server()
        self.create_serverside_folder()

    def extract(self):
        self.set_pg_path()
        try:
            with Connection(login=self.login) as conn:
                self.conn = conn
                self.create_foreign_schema()
                self.conn.commit()
                if self.boundary:
                    self.set_target_boundary(self.boundary,
                                             name=self.boundary_name)
                self.update_boundaries()
                for tn, geom in self.tables.items():
                    self.extract_table(tn, geom)
                self.additional_stuff()
                self.conn.commit()
                self.final_stuff()
                self.conn.commit()
        except Exception as e:
            raise(e)
        finally:
            self.cleanup()
        self.further_stuff()

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
        OPTIONS (
            host '{self.foreign_login.host}',
            port '{self.foreign_login.port}', dbname '{self.foreign_login.db}',
            fetch_size '100000',
            extensions 'postgis, hstore, postgis_raster',
            updatable 'false'
        );
        -- user
        CREATE USER MAPPING FOR {self.login.user}
        SERVER {self.foreign_server}
        OPTIONS (user '{self.foreign_login.user}',
        password '{self.foreign_login.password}');
        """
        self.logger.info(
            f'Creating connection to database "{self.foreign_login.db}"')
        with Connection(login=self.login) as conn:
            self.run_query(sql, conn=conn, verbose=False)

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
        CREATE EXTENSION IF NOT EXISTS plpython3u;
        CREATE EXTENSION IF NOT EXISTS kmeans;
        CREATE EXTENSION IF NOT EXISTS postgres_fdw;
        CREATE OR REPLACE AGGREGATE public.hstore_sum (public.hstore)
        (
          SFUNC = public.hs_concat,
          STYPE = public.hstore
        );
        CREATE OR REPLACE FUNCTION clone_schema(source_schema text, dest_schema text) RETURNS void AS
        $BODY$
        DECLARE
          objeto text;
          buffer text;
        BEGIN
            EXECUTE 'CREATE SCHEMA ' || dest_schema ;

            FOR objeto IN
                SELECT table_name::text FROM information_schema.tables WHERE table_schema = source_schema
            LOOP
                buffer := dest_schema || '.' || objeto;
                EXECUTE 'CREATE TABLE ' || buffer || ' (LIKE ' || source_schema || '.' || objeto || ' INCLUDING CONSTRAINTS INCLUDING INDEXES INCLUDING DEFAULTS)';
                EXECUTE 'INSERT INTO ' || buffer || '(SELECT * FROM ' || source_schema || '.' || objeto || ')';
            END LOOP;

        END;
        $BODY$
        LANGUAGE plpgsql VOLATILE;
        '''
        self.logger.info('Adding extensions')
        with Connection(login=self.foreign_login) as conn0:
            self.run_query(sql, conn=conn0)

    def final_stuff(self):
        """
        CREATE INDEX in the target DB
        """
        self.create_index()

    def create_index(self):
        """
        To be defined in the subclass
        """

    def extract_table(self, tn, boundary_name='bbox', geom='geom'):
        """
        extracts a single table
        """
        geometrytype = self.get_geometrytype(tn, geom)
        cols = self.conn.get_column_dict(tn, self.temp)
        cols_without_geom = ('t."{}"'.format(c) for c in cols if c != geom)
        col_str = ', '.join(cols_without_geom)

        sql = SQL(f"""
SELECT {col_str}, st_transform(t.{geom}, {srid})::geometry({geometrytype}, {self.target_srid}) as geom
INTO {self.schema}.{tn}
FROM {self.temp}.{tn} t, meta.boundary tb
WHERE
tb.name = {area_name}
AND
st_intersects(t.{geom}, tb.source_geom)
        """).format(area_name=Literal(boundary_name))
        self.run_query(sql, conn=self.conn)

    def get_geometrytype(self, tn, geom):
        sql = """
SELECT geometrytype({geom}) FROM {sn}.{tn} LIMIT 1;
        """.format(geom=geom, sn=self.temp, tn=tn)
        cur = self.conn.cursor()
        cur.execute(sql)
        geometrytype = cur.fetchone()[0]
        return geometrytype

    def create_foreign_schema(self, foreign_schema=None, target_schema=None):
        """
        links schema in database to schema on foreign server
        """
        foreign_schema = foreign_schema or self.foreign_schema or self.schema
        target_schema = target_schema or self.temp
        sql = f'''
        DROP SCHEMA IF EXISTS {self.schema} CASCADE;
        CREATE SCHEMA {self.schema};
        '''
        self.run_query(sql, conn=self.conn)
        sql = f"""
        DROP SCHEMA IF EXISTS {target_schema} CASCADE;
        CREATE SCHEMA {target_schema};
        """
        self.run_query(sql, conn=self.conn)

        sql = f"""
        IMPORT FOREIGN SCHEMA {foreign_schema}
        FROM SERVER {self.foreign_server} INTO {target_schema};
        """

        self.run_query(sql, self.conn)

    def get_target_boundary(self, name=None):
        """
        get the target boundary from the destination database
        """
        with Connection(login=self.login) as conn:
            cur = conn.cursor()
            sql = f"""
            SELECT ST_AsText(source_geom) as wkt FROM meta.boundary
            WHERE name='{name or self.boundary_name}';
            """
            cur.execute(sql)
            row = cur.fetchone()
            return row.wkt

    def get_target_srid(self):
        """
        get the target boundary from the destination database

        Returns
        -------
        srid : int
        """
        try:
            with Connection(login=self.login) as conn:
                cur = conn.cursor()
                sql = """
    SELECT
        st_srid(a.geom) AS srid
    FROM meta.boundary a;
    """
                cur.execute(sql)
                row = cur.fetchone()
                srid = row.srid
        except errors.UndefinedTable:
            return
        return srid

    def set_target_boundary(self, geom, name='bbox'):
        """
        insert or replace geometry of entry with given name in boundary table
        """
        wkt = geom.ExportToWkt()
        s_ref = geom.GetSpatialReference()
        if s_ref:
            srid = s_ref.GetAuthorityCode(None)
            if srid != str(self.srid):
                raise Exception('Provided geometry has wrong projection {srid}.'
                                ' Projection should be {self.srid} instead!')
        sql = '''
        INSERT INTO meta.boundary (name, source_geom)
        VALUES ('{name}', st_setsrid(ST_GeomFromText('{wkt}'), {srid}))
        ON CONFLICT (name) DO
        UPDATE SET source_geom=st_setsrid(ST_GeomFromText('{wkt}'), {srid});
        '''.format(wkt=wkt, srid=self.srid, name=name)
        with Connection(login=self.login) as conn:
            self.run_query(sql, conn)

    def update_boundaries(self):
        sql = '''
        UPDATE meta.boundary
        SET geom = st_transform(source_geom, {target_srid});
        '''.format(target_srid=self.target_srid)
        with Connection(login=self.login) as conn:
            self.run_query(sql, conn)

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
        """).format(db=Identifier(self.login.db), role=Identifier(self.role))

        login = deepcopy(self.login)
        login.db = 'postgres'
        with Connection(login=login) as conn:
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            self.run_query(sql, conn=conn)

    def create_meta(self):
        sql = '''
        CREATE SCHEMA meta;
        DROP TABLE IF EXISTS meta.boundary;
        CREATE TABLE meta.boundary (name VARCHAR PRIMARY KEY,
                                    source_geom geometry('MULTIPOLYGON', {srid}),
                                    geom geometry('MULTIPOLYGON', {target_srid}));
        '''.format(srid=self.srid, target_srid=self.target_srid)
        with Connection(login=self.login) as conn:
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
        #with Connection(login=self.login) as conn:
            #self.run_query(sql, conn=conn)
            #conn.commit()

    def cleanup(self, schema=None):
        """
        remove the temp schema
        """
        with Connection(login=self.foreign_login) as conn0:
            sql = '''DROP SCHEMA IF EXISTS {temp} CASCADE'''.format(
                temp=schema or self.temp)
            self.run_query(sql, conn=conn0)

    def cluster_and_analyse(self):
        """
        """
        login = self.login

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
            self.logger.info(cmd)
            subprocess.call(cmd, shell=self.SHELL)
        else:
            self.logger.info('no tables to cluster')

        vacuumdb = os.path.join(self.PGPATH, 'vacuumdb')
        cmd = '''"{vacuumdb}" -U {user} -h {host} -p {port} -w --analyze-in-stages -d {destination_db}'''
        cmd = cmd.format(vacuumdb=vacuumdb,
                         destination_db=login.db,
                         user=login.user,
                         port=login.port,
                         host=login.host)
        self.logger.info(cmd)
        subprocess.call(cmd, shell=self.SHELL)

    def create_serverside_folder(self):
        """ Create a project folder on the server"""
        self.check_platform()
        folder = os.path.join(self.folder, 'projekte', self.destination_db)
        self.make_folder(folder)

    def copy_schema_to_target_db(self, schema):
        temp_schema = round(time.time());
        self.create_foreign_schema(foreign_schema=schema,
                                   target_schema=temp_schema)
        sql = f"SELECT clone_schema('{temp_schema}','{schema}');"
        with Connection(login=self.login) as conn:
            self.run_query(sql, conn=conn)
        self.cleanup(schema=temp_schema)

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
    extract.add_target_boundary(bbox)
    extract.extract()

