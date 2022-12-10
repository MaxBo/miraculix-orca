#!/usr/bin/env python
# coding:utf-8

from argparse import ArgumentParser

import sys
import os
import re
import subprocess
import time
from typing import List
from osgeo import ogr
from psycopg2.sql import Identifier, SQL
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
                 target_srid: int = None,
                 temp: str = None,
                 foreign_server: str = 'foreign_server',
                 foreign_login: Login = None,
                 tables: dict = {},
                 source_db: str = None,
                 logger=None,
                 boundary: ogr.Geometry = None,
                 boundary_name: str = 'bbox',
                 **options):
        self.srid = 4326
        self.logger = logger or logging.getLogger(self.__module__)
        self.session_id = f'{destination_db}{round(time.time() * 100)}'
        self.temp = temp or 'temp'  # f'temp{self.session_id}'
        self.foreign_server = foreign_server
        self.source_db = source_db or os.environ.get(
            'FOREIGN_NAME', 'europe')
        self.destination_db = destination_db
        if tables:
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
        exists = self.check_if_database_exists(self.destination_db)
        if exists:
            self.logger.info(f'Truncate Database {self.destination_db}')
            self.truncate_db()
        else:
            self.logger.info(f'Create Database {self.destination_db}')
            self.create_target_db(self.login)
            self.logger.info(f'Create Database {self.destination_db}')
        self.create_extensions()
        self.create_meta()
        self.create_foreign_server()
        self.create_serverside_folder()

    def extract(self):
        self.set_pg_path()
        try:
            with Connection(login=self.login) as conn:
                self.conn = conn
                if self.boundary:
                    self.set_target_boundary(self.boundary,
                                             name=self.boundary_name)
                self.update_boundaries()
                self.create_schema(self.schema, conn=conn, replace=True)
                self.create_foreign_catalog()
                self.create_foreign_schema()
                self.conn.commit()
                for tn, geom in self.tables.items():
                    self.extract_table(tn, geom=geom)
                self.additional_stuff()
                self.conn.commit()
                self.final_stuff()
                self.conn.commit()
                self.further_stuff()
        except Exception as e:
            raise(e)
        finally:
            with Connection(login=self.login) as conn:
                self.cleanup(conn=conn)
                self.cleanup(conn=conn, schema='temp_pg_catalog')

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
            updatable 'true'
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
            self.run_query(sql, conn=conn)

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
        '''
        self.logger.info('Adding extensions')
        with Connection(login=self.login) as conn:
            self.run_query(sql, conn=conn)

    def final_stuff(self):
        """
        CREATE INDEX in the target DB
        """
        self.create_index()

    def create_index(self):
        """
        To be defined in the subclass
        """

    def extract_table(self, tn, geom='geom', boundary_name=None):
        """
        extracts a single table
        """
        cols = self.conn.get_column_dict(tn, self.temp)

        if geom is None:
            cols = ('t."{}"'.format(c)
                    for c in cols)
            col_str = ', '.join(cols)
            sql = f"""
SELECT {col_str}
INTO {self.schema}.{tn}
FROM {self.temp}.{tn} t;
            """
        else:

            wkt = self.get_target_boundary(
                boundary_name=boundary_name or self.boundary_name)
            geometrytype = self.get_geometrytype(tn, geom)
            cols_without_geom = ('t."{}"'.format(c)
                                 for c in cols if c != geom)
            col_str = ', '.join(cols_without_geom)

            sql = f"""
            SELECT
              {col_str},
              st_transform(t.{geom}, {self.target_srid})::geometry({geometrytype},
              {self.target_srid}) as geom
            INTO {self.schema}.{tn}
            FROM {self.temp}.{tn} t,
            (SELECT ST_GeomFromEWKT('SRID={self.srid};{wkt}') AS source_geom) tb
            WHERE
            st_intersects(t.{geom}, tb.source_geom);
            """

        self.logger.info(f'Extracting table "{tn}" into {self.schema}.{tn}')
        self.run_query(sql, conn=self.conn)

        description = self.get_description(
            tn, self.foreign_schema or self.schema, foreign=True)
        if description:
            sql = f'''
            COMMENT ON TABLE {self.schema}.{tn} IS '{description}';
            '''
            self.run_query(sql, conn=self.conn)

    def get_description(self, relation, schema, foreign=False, conn=None):
        cat = 'temp_pg_catalog' if foreign else 'pg_catalog'
        sql = f'''
        SELECT d.description FROM {cat}.pg_description as d
        join {cat}.pg_class as c on d.objoid = c.oid
        join {cat}.pg_namespace as n on c.relnamespace = n.oid
        where relname='{relation}' and nspname='{schema}';
        '''
        conn = conn or self.conn
        cur = conn.cursor()
        cur.execute(sql)
        description = cur.fetchone()
        return description[0] if description else None

    def get_relkinds(self, schema, foreign=False, conn=None) -> dict:
        cat = 'temp_pg_catalog' if foreign else 'pg_catalog'
        sql = f'''
        SELECT c.relname, c.relkind FROM {cat}.pg_class as c
        join {cat}.pg_namespace as n on c.relnamespace = n.oid
        where nspname='{schema}';
        '''
        conn = conn or self.conn
        cur = conn.cursor()
        cur.execute(sql)
        relkinds = cur.fetchall()
        return dict(relkinds)

    def get_geometrytype(self, tn, geom):
        sql = """
        SELECT geometrytype({geom}) FROM {sn}.{tn} LIMIT 1;
        """.format(geom=geom, sn=self.temp, tn=tn)
        cur = self.conn.cursor()
        cur.execute(sql)
        geometrytype = cur.fetchone()[0]
        return geometrytype

    def create_foreign_schema(self,
                              foreign_schema=None,
                              target_schema=None,
                              tables: list = None,
                              conn=None):
        """
        links schema in database to schema on foreign server
        """
        conn = conn or self.conn
        foreign_schema = foreign_schema or self.foreign_schema or self.schema
        target_schema = target_schema or self.temp
        sql = f"""
        DROP SCHEMA IF EXISTS {target_schema} CASCADE;
        CREATE SCHEMA {target_schema};
        """
        self.run_query(sql, conn=conn)

        sql = f'IMPORT FOREIGN SCHEMA {foreign_schema} '
        if tables:
            sql += f'LIMIT TO ({",".join(tables)}) '
        sql += f'FROM SERVER {self.foreign_server} INTO {target_schema};'
        self.run_query(sql, conn=conn)

    def create_foreign_catalog(self):
        self.create_foreign_schema(
            foreign_schema='pg_catalog',
            target_schema='temp_pg_catalog',
            tables=['pg_description', 'pg_class', 'pg_namespace', 'pg_views']
        )

    def get_target_boundary(self, boundary_name=None):
        """
        get the target boundary from the destination database
        """
        with Connection(login=self.login) as conn:
            cur = conn.cursor()
            sql = f"""
            SELECT ST_AsText(source_geom) as wkt FROM meta.boundary
            WHERE name='{boundary_name or self.boundary_name}';
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

    def grant_access(self, users):
        with Connection(login=self.login) as conn:
            sql = 'SELECT schema_name FROM information_schema.schemata;'
            cur = conn.cursor()
            cur.execute(sql)
            rows = cur.fetchall()
            schemas = [r.schema_name for r in rows if not (
                r.schema_name.startswith('pg_') or
                r.schema_name == 'information_schema')]
            for schema in schemas:
                for user in users:
                    self.logger.info(f'granting access to schema "{schema}" to '
                                     f'user "{user}"')
                    sql = f'''
                    GRANT USAGE ON SCHEMA {schema} TO {user};
                    GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA {schema} TO {user};
                    GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA {schema} TO {user};
                    ALTER DEFAULT PRIVILEGES IN SCHEMA {schema} GRANT INSERT, SELECT, UPDATE, DELETE, TRUNCATE ON TABLES TO {user};
                    '''
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
        # sql = """
# SELECT pg_cancel_backend(pid) FROM pg_stat_activity WHERE datname = '{db}';
        # """.format(db=self.destination_db)
        # with Connection(login=self.login) as conn:
        #self.run_query(sql, conn=conn)
        # conn.commit()

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

    def cleanup(self, schema=None, conn=None):
        """
        remove the temp schema
        """
        self.logger.info(f'Cleaning up...')
        sql = '''DROP SCHEMA IF EXISTS {temp} CASCADE'''.format(
            temp=schema or self.temp)
        try:
            self.run_query(sql, conn=conn or self.conn)
        except errors.UndefinedTable:
            pass

    def vacuum(self, schema=None, tables=[]):
        """
        """
        login = self.login

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

    def copy_tables_to_target_db(self,
                                 schema: str = None,
                                 tables: list = None,
                                 conn=None,
                                 skip_views=True):
        schema = schema or self.schema
        conn = conn or self.conn
        temp_schema = f'class_temp'
        self.create_schema(schema, conn=conn)
        self.create_foreign_catalog()
        self.create_foreign_schema(foreign_schema=schema,
                                   target_schema=temp_schema,
                                   conn=conn)

        cur = conn.cursor()
        if not tables:
            sql = f'''
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = '{temp_schema}';
            '''
            self.logger.info(sql)
            cur.execute(sql)
            rows = cur.fetchall()
            tables = [row[0] for row in rows]

        relkinds = self.get_relkinds(schema=schema, foreign=True)

        sql = f'''CREATE SCHEMA IF NOT EXISTS {schema};'''
        self.run_query(sql, conn=conn)
        for table in tables:
            if skip_views and relkinds[table] == 'v':
                self.logger.info(f'Skip View "{table}"')
                continue
            self.logger.info(f'Copying table "{table}" to {schema}.{table}')
            sql = f'''
            DROP TABLE IF EXISTS {schema}.{table};
            CREATE TABLE {schema}.{table} (LIKE {temp_schema}.{table}
            INCLUDING CONSTRAINTS INCLUDING INDEXES INCLUDING DEFAULTS);
            INSERT INTO {schema}.{table} SELECT * FROM {temp_schema}.{table};
            '''
            self.run_query(sql, conn=conn)
            description = self.get_description(
                table, self.foreign_schema or schema, foreign=True)
            if description:
                sql = f'''
                COMMENT ON TABLE {schema}.{table} IS '{description}';
                '''
                self.run_query(sql, conn=self.conn)

        self.cleanup(schema=temp_schema, conn=conn)

    def copy_views_to_target_db(self,
                                schema: str = None,
                                views: list = None,
                                conn=None):
        """copy the views to the target database"""
        cat = 'temp_pg_catalog'
        sql = f'''SELECT viewname,  definition
        FROM {cat}.pg_views
        WHERE schemaname = '{schema}';'''
        conn = conn or self.conn
        cur = conn.cursor()
        cur.execute(sql)
        view_definitions = cur.fetchall()

        # pattern to find casts to geometry in WGS84
        # ::[public.]geometry(POINT|POLYGON, 4326)
        pattern = '(\:\:(?:\w+\.)?geometry\(.*,\s*)4326(\s*\))'

        for viewname, definition in view_definitions:
            # replace casts to geometry with the target srid
            definition = re.sub(pattern,
                                f'\g<1>{self.target_srid}\g<2>',
                                definition,
                                flags=re.M)

            sql = f'''CREATE OR REPLACE VIEW {schema}."{viewname}" AS
            {definition}'''
            cur.execute(sql)


    def copy_layer_styles(self, schema: str, tables: List[str]):
        """copy layer styles for tables in schema"""
        temp_public = 'temp_public'
        layer_styles = 'layer_styles'
        self.create_foreign_schema(foreign_schema='public',
                                   target_schema=temp_public,
                                   tables=[layer_styles],)
        # check if layer_styles exists in target_db
        sql = f"SELECT to_regclass('{layer_styles}')::oid;"
        cur = self.conn.cursor()
        cur.execute(sql)
        oid = cur.fetchone()[0]
        # if it does not exist, create the table
        if oid is None:
            sql = f'''
            CREATE TABLE {layer_styles} (LIKE {temp_public}.{layer_styles}
            INCLUDING ALL);
            ALTER TABLE {layer_styles} ALTER COLUMN id TYPE serial;
            '''
            cur.execute(sql)

        # copy the styles
        sql = f'''
        INSERT INTO {layer_styles} (
        f_table_catalog,
        f_table_schema,
        f_table_name,
        f_geometry_column,
        stylename,
        styleqml,
        stylesld,
        useasdefault,
        description,
        ui,
        update_time,
        type
        )
        SELECT
        '{self.destination_db}' AS f_table_catalog,
        l.f_table_schema,
        l.f_table_name,
        l.f_geometry_column,
        l.stylename,
        l.styleqml,
        l.stylesld,
        l.useasdefault,
        l.description,
        l.ui,
        l.update_time,
        l.type
        FROM {temp_public}.layer_styles l
        WHERE l.f_table_schema = %s AND l.f_table_name = ANY(%s);
        '''
        cur.execute(sql, (schema, tables))
        self.cleanup(schema=temp_public, conn=self.conn)


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

