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
                 destination_db='extract',
                 temp='temp',
                 target_srid=31467,
                 **options):
        self.srid = 4326
        self.target_srid = target_srid
        self.temp = temp
        self.source_db = options.get('source_db', 'europe')
        self.destination_db = destination_db
        self.tables2cluster = []
        self.check_platform()

    def set_login(self, host, port, user, password=None, **kwargs):
        """
        set login information for source and destination database
        to self.login0 and self.login1

        Parameters
        ----------
        host : str
        port : int
        user : str
        password : str, optional
        """
        self.login0 = Login(host, port, user, password, db=self.source_db)
        self.login1 = Login(host, port, user, password,
                            db=self.destination_db)

    def set_login01(self, login: Login, source_db):
        """
        set login information for source and destination database
        to self.login0 and self.login1

        Parameters
        ----------
        login : Login
        source_db : str
        """
        self.login0 = Login(login.host, login.port,
                            login.user, login.password,
                            db=source_db)
        self.login1 = login

    def execute_query(self, sql):
        """
        establishes a connection and executes some code
        """
        with Connection(login=self.login0) as conn0, \
                Connection(login=self.login1) as conn1:
            self.run_query(sql)

    def recreate_db(self):
        self.set_pg_path()
        exists = self.check_if_database_exists(self.destination_db)
        if exists:
            logger.info(f'Truncate Database {self.destination_db}')
            self.truncate_db()
        else:
            logger.info(f'Create Database {self.destination_db}')
            self.create_target_db(self.login1)
            logger.info(f'Create Database {self.destination_db}')
        self.create_serverside_folder()

    def extract(self):
        self.set_pg_path()
        with Connection(login=self.login0) as conn0, \
                Connection(login=self.login1) as conn1:
            self.conn0 = conn0
            self.conn1 = conn1
            self.set_session_authorization(self.conn0)
            self.set_search_path('conn0')
            self.create_temp_schema()
            self.create_target_boundary()
            for tn, geom in self.tables.items():
                self.extract_table(tn, geom)
            self.additional_stuff()
            self.conn0.commit()
            self.reset_authorization(self.conn0)
            self.conn1.commit()

            self.copy_temp_schema_to_target_db(schema=self.temp)
            self.rename_schema()
            self.final_stuff()
            self.cleanup()
            self.conn0.commit()
            self.conn1.commit()

        self.further_stuff()

    def further_stuff(self):
        """
        To be defined in the subclass
        """

    def additional_stuff(self):
        """
        additional steps, to be defined in the subclass
        """

    def create_extensions(self):
        """
        """
        # Rename restored temp-Schema to new name
        sql = '''
        CREATE EXTENSION IF NOT EXISTS dblink;
        CREATE EXTENSION IF NOT EXISTS postgis;
        CREATE EXTENSION IF NOT EXISTS postgis_raster;
        CREATE EXTENSION IF NOT EXISTS hstore;
        CREATE EXTENSION IF NOT EXISTS pgRouting;
        CREATE EXTENSION IF NOT EXISTS kmeans;
        CREATE EXTENSION IF NOT EXISTS plpython3u;
        DROP AGGREGATE IF EXISTS public.hstore_sum(public.hstore);
        CREATE AGGREGATE public.hstore_sum (
        public.hstore)
      (
        SFUNC = public.hs_concat,
        STYPE = public.hstore
      );
        '''
        with Connection(login=self.login0) as conn0:
            self.run_query(sql, conn=conn0)

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

    def create_temp_schema(self):
        """
        Creates a temporary schema
        """
        sql = '''
DROP SCHEMA IF EXISTS {temp} CASCADE;
CREATE SCHEMA {temp};
        '''.format(temp=self.temp)

        self.run_query(sql, self.conn0)

        sql = """
DROP SCHEMA IF EXISTS {schema} CASCADE;
        """.format(schema=self.schema)
        self.run_query(sql, conn=self.conn1)

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
        sql = '''
DROP TABLE IF EXISTS {temp}.boundary;
CREATE TABLE {temp}.boundary (id INTEGER PRIMARY KEY,
                              source_geom geometry('POLYGON', {srid}),
                              geom geometry('POLYGON', {target_srid}));
INSERT INTO {temp}.boundary (id, source_geom)
VALUES (1, st_setsrid(st_makebox2d(st_point({LEFT}, {TOP}), st_point({RIGHT}, {BOTTOM})), {srid}));
UPDATE {temp}.boundary SET geom = st_transform(source_geom, {target_srid});
'''.format(temp=self.temp,
           LEFT=bbox.left, RIGHT=bbox.right,
           TOP=bbox.top, BOTTOM=bbox.bottom,
           srid=self.srid,
           target_srid=self.target_srid)
        self.run_query(sql, self.conn0)

    def set_pg_path(self):
        """"""
        if sys.platform.startswith('win'):
            self.PGPATH = r'C:\Program Files\PostgreSQL\9.3\bin'
            self.SHELL = False
        else:
            self.PGPATH = '/usr/bin'
            self.SHELL = True

    def create_target_db(self, login):
        """
        create the target database
        """
        createdb = os.path.join(self.PGPATH, 'createdb')

        cmd = '''"{createdb}" -U {user} -h {host} -p {port} -w -T pg_template {destination_db}'''.format(createdb=createdb, destination_db=login.db, user=login.user,
                                                                                                         port=login.port, host=login.host)
        logger.info(cmd)
        ret = subprocess.call(cmd, shell=self.SHELL)
        if ret:
            raise IOError(
                'Database {db} could not be recreated'.format(db=login.db))

        sql = """
ALTER DATABASE {db} OWNER TO {role};
        """
        with Connection(login=self.login0) as conn0:
            self.run_query(sql.format(db=login.db,
                                      role=self.role),
                           conn=conn0)
            conn0.commit()
        self.create_extensions()

    def truncate_db(self):
        """Truncate the database"""
        sql = """
SELECT pg_cancel_backend(pid) FROM pg_stat_activity WHERE datname = '{db}';
        """.format(db=self.destination_db)
        with Connection(login=self.login0) as conn:
            self.run_query(sql, conn=conn)
            conn.commit()

        sql = """
SELECT 'drop schema if exists "' || schema_name || '" cascade;' AS sql
FROM (SELECT catalog_name, schema_name
      FROM information_schema.schemata
      WHERE schema_name not like 'pg_%' AND
      schema_name not IN ('information_schema', 'public',
                          'topology', 'repack')) s;
        """
        with Connection(login=self.login1) as conn:
            cursor2 = conn.cursor()
            cursor = conn.cursor()
            cursor.execute(sql)
            for row in cursor:
                cursor2.execute(row.sql)
            conn.commit()

        sql = """
update pg_database set datallowconn = 'True' where datname = '{db}';
        """.format(db=self.destination_db)
        with Connection(login=self.login0) as conn:
            self.run_query(sql, conn=conn)
            conn.commit()

    def copy_temp_schema_to_target_db(self, schema):
        """
        copy the temp_schema to the target db
        build a pipe between the Databases to copy the temp_schema into the
        osm_schema in the new Database
        """
        pg_dump = os.path.join(self.PGPATH, 'pg_dump')
        pg_restore = os.path.join(self.PGPATH, 'pg_restore')

        logger.info('copy schema {schema} from {db0} to {db1}'.format(schema=schema,
                                                                      db0=self.login0.db,
                                                                      db1=self.login1.db))

        logger.info('login_source: %s' % self.login0)
        logger.info('login_dest: %s' % self.login1)

        pg_dump_cmd = ' '.join([
            '"{cmd}"'.format(cmd=pg_dump),
            '--host={host}'.format(host=self.login0.host),
            '--port={port}'.format(port=self.login0.port),
            '--username={user}'.format(user=self.login0.user),
            '-w',
            '--format=custom',
            '--verbose',
            '--schema={schema}'.format(schema=schema),
            '{db}'.format(db=self.login0.db),
        ])

        logger.info(pg_dump_cmd)

        dump = subprocess.Popen(pg_dump_cmd,
                                stdout=subprocess.PIPE,
                                shell=self.SHELL,
                                )

        pg_restore_cmd = ' '.join([
            '"{cmd}"'.format(cmd=pg_restore),
            '-d {db}'.format(db=self.login1.db),
            '--host={host}'.format(host=self.login1.host),
            '--port={port}'.format(port=self.login1.port),
            '--username={user}'.format(user=self.login1.user),
            # '--clean',
            '-w',
            '--format=custom',
            '--verbose',
        ])
        logger.info(pg_restore_cmd)

        try:

            restore = subprocess.check_output(pg_restore_cmd,
                                              stdin=dump.stdout,
                                              shell=self.SHELL)
        except subprocess.CalledProcessError as err:
            logger.info(err)

        dump.terminate()

    def cleanup(self):
        """
        remove the temp schema
        """
        sql = '''DROP SCHEMA IF EXISTS {temp} CASCADE'''.format(
            temp=self.temp)
        self.run_query(sql, conn=self.conn0)

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


class ExtractMeta(Extract):
    """
    Create the target DB and Extract the Meta Tables
    """
    schema = 'meta'

    def get_credentials(self):
        """get credentials for db_link user"""
        sql = """
SELECT key, value FROM meta_master.credentials;
        """
        cursor = self.conn0.cursor()
        cursor.execute(sql)
        rows = cursor.fetchall()
        credentials = dict(row for row in rows)
        return credentials

    def additional_stuff(self):
        """
        additional steps, to be defined in the subclass
        """
        cursor = self.conn0.cursor()

        credentials = self.get_credentials()

        # create dblink tables
        sql = """
CREATE OR REPLACE FUNCTION {temp}.select_master_scripts()
  RETURNS TABLE (id integer, scriptcode text, scriptname text,
         description text, parameter text, category text) AS
$BODY$
DECLARE
BEGIN
perform dblink_connect_u('conn', 'host=localhost dbname={sd} user={source_user}');
RETURN QUERY
SELECT *
FROM dblink('conn',
'SELECT id, scriptcode, scriptname, description, parameter, category
FROM meta_master.scripts'
  ) AS m(id integer,
         scriptcode text,
         scriptname text,
         description text,
         parameter text,
         category text);
perform dblink_disconnect('conn');
END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;

CREATE OR REPLACE VIEW {temp}.master_scripts AS
SELECT *
FROM {temp}.select_master_scripts();

CREATE OR REPLACE FUNCTION {temp}.select_dependencies()
  RETURNS TABLE (scriptcode text,
         needs_script text) AS
$BODY$
DECLARE
BEGIN
perform dblink_connect_u('conn', 'host=localhost dbname={sd} user={source_user}');
RETURN QUERY
SELECT *
FROM dblink('conn',
'SELECT scriptcode, needs_script
   FROM meta_master.dependencies'
  ) AS m(scriptcode text,
         needs_script text);
perform dblink_disconnect('conn');
END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;

CREATE OR REPLACE VIEW {temp}.master_dependencies AS
SELECT *
FROM {temp}.select_dependencies();

CREATE TABLE {temp}.local_dependencies
(
  scriptcode text NOT NULL,
  needs_script text NOT NULL,
  CONSTRAINT local_dependencies_pkey PRIMARY KEY (scriptcode, needs_script)
);

CREATE OR REPLACE VIEW {temp}.dependencies AS
 SELECT master_dependencies.scriptcode,
    master_dependencies.needs_script
   FROM {temp}.master_dependencies
UNION
 SELECT local_dependencies.scriptcode,
    local_dependencies.needs_script
   FROM {temp}.local_dependencies;
        """
        cursor.execute(sql.format(temp=self.temp, sd=self.source_db,
                                  source_user=credentials['user'],
                                  source_pw=credentials['password']))

        sql = """
CREATE OR REPLACE FUNCTION {temp}.select_dependent_scripts()
  RETURNS trigger AS
$BODY$
DECLARE
BEGIN
  IF NEW.todo
  THEN
  UPDATE meta.scripts s
  SET todo = TRUE
  FROM meta.dependencies d
  WHERE d.needs_script = s.scriptcode
  AND d.scriptcode = NEW.scriptcode
  AND s.success IS NOT True;
  END IF;
  RETURN NEW;

END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
        """
        cursor.execute(sql.format(temp=self.temp))

        sql = """
CREATE OR REPLACE FUNCTION {temp}.unselect_dependent_scripts()
  RETURNS trigger AS
$BODY$
DECLARE
BEGIN
  IF NOT NEW.todo AND NEW.success IS NOT True
  THEN
  UPDATE meta.scripts s
  SET todo = False
  FROM meta.dependencies d
  WHERE d.scriptcode = s.scriptcode
  AND d.needs_script = NEW.scriptcode;
  END IF;
  RETURN NEW;

END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
        """
        cursor.execute(sql.format(temp=self.temp))

        sql = """
CREATE OR REPLACE FUNCTION {temp}.check_script_id()
  RETURNS trigger AS
$BODY$
DECLARE mid integer;
BEGIN
  SELECT m.id INTO mid FROM meta.master_scripts m WHERE m.scriptcode = NEW.scriptcode;
  IF NOT mid IS NULL THEN
      NEW.id := mid;
  END IF;
  RETURN NEW;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
        """
        cursor.execute(sql.format(temp=self.temp))

        sql = '''
CREATE TABLE {temp}.scripts
( id integer,
  scriptcode text,
  started boolean NOT NULL DEFAULT false,
  success boolean DEFAULT NULL,
  starttime timestamp with time zone,
  endtime timestamp with time zone,
  todo boolean NOT NULL DEFAULT false,
  CONSTRAINT scripts_pkey PRIMARY KEY (id),
  CONSTRAINT scripts_scriptcode_key UNIQUE (scriptcode)
);

CREATE TRIGGER scripts_select_trigger
  AFTER INSERT OR UPDATE OF todo
  ON {temp}.scripts
  FOR EACH ROW
  EXECUTE PROCEDURE {temp}.select_dependent_scripts();

CREATE TRIGGER scripts_unselect_trigger
  AFTER UPDATE OF todo
  ON {temp}.scripts
  FOR EACH ROW
  EXECUTE PROCEDURE {temp}.unselect_dependent_scripts();

CREATE TRIGGER scripts_update_trigger
  BEFORE UPDATE
  ON {temp}.scripts
  FOR EACH ROW
  EXECUTE PROCEDURE {temp}.check_script_id();
'''.format(schema=self.schema, temp=self.temp)
        self.run_query(sql, conn=self.conn0)
        self.conn0.commit()

        sql = '''
CREATE OR REPLACE FUNCTION {temp}.check_scriptcode()
  RETURNS trigger AS
$BODY$
DECLARE
BEGIN
  IF EXISTS(SELECT 1 FROM meta.master_scripts m WHERE m.scriptcode = NEW.scriptcode)
  THEN
    RAISE EXCEPTION 'scriptcode % already in meta.master_scripts', NEW.scriptcode;
  END IF;
  RETURN NEW;
END;
$BODY$
LANGUAGE 'plpgsql'
VOLATILE
CALLED ON NULL INPUT
SECURITY INVOKER
COST 100;
    '''
        cursor.execute(sql.format(temp=self.temp))

        sql = '''
CREATE SEQUENCE {temp}.local_scripts_id_seq
  INCREMENT 1 MINVALUE 1000
  MAXVALUE 2147483647 START 1000
  CACHE 1;
ALTER SEQUENCE {temp}.local_scripts_id_seq RESTART WITH 1000;

CREATE TABLE {temp}.local_scripts
(
  id integer NOT NULL DEFAULT nextval(('meta.local_scripts_id_seq'::text)::regclass),
  scriptcode text,
  scriptname text,
  description text,
  parameter text,
  category text,
  CONSTRAINT local_scripts_pkey PRIMARY KEY (id),
  CONSTRAINT local_scripts_scriptcode_key UNIQUE (scriptcode)
)
WITH (
  OIDS=FALSE
);

CREATE TRIGGER check_scriptcode_tr
  BEFORE INSERT OR UPDATE OF scriptcode
  ON {temp}.local_scripts FOR EACH ROW
  EXECUTE PROCEDURE {temp}.check_scriptcode();

CREATE OR REPLACE VIEW {temp}.all_scripts(
    id,
    scriptcode,
    scriptname,
    description,
    parameter,
    category,
    source)
AS
  SELECT master_scripts.id,
         master_scripts.scriptcode,
         master_scripts.scriptname,
         master_scripts.description,
         master_scripts.parameter,
         master_scripts.category,
         'm'::text AS source
  FROM {temp}.master_scripts
  UNION ALL
  SELECT local_scripts.id::integer,
         local_scripts.scriptcode,
         local_scripts.scriptname,
         local_scripts.description,
         local_scripts.parameter,
         local_scripts.category,
         'l'::text AS source
  FROM {temp}.local_scripts;

CREATE OR REPLACE VIEW {temp}.script_view AS
SELECT
    m.id,
    m.scriptcode,
    m.scriptname,
    m.description,
    m.parameter,
    m.category,
    s.started,
    s.success,
    s.starttime,
    s.endtime,
    s.todo
FROM
    {temp}.all_scripts m
    LEFT JOIN {temp}.scripts s ON m.scriptcode = s.scriptcode
ORDER BY m.id
;
        '''.format(temp=self.temp)
        self.run_query(sql, conn=self.conn0)
        self.conn0.commit()

    def final_stuff(self):
        """Final things to do"""
        sql = '''
INSERT INTO {schema}.scripts (id, scriptcode)
SELECT id, scriptcode FROM {schema}.all_scripts;
    '''.format(schema=self.schema)
        self.run_query(sql, conn=self.conn1)


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

