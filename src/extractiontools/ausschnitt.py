#!/usr/bin/env python
#coding:utf-8

from argparse import ArgumentParser

import numpy as np
import logging
logger = logging.getLogger()
logger.addHandler(logging.StreamHandler())
logger.level = logging.DEBUG
import sys
import os
import subprocess

from connection import Connection, DBApp, Login


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
                 recreate_db=False,
                 **options):
        self.srid = 4326
        self.target_srid = target_srid
        self.temp = temp
        self.source_db = options.get('source_db', 'dplus')
        self.destination_db = destination_db
        self.recreate_db = recreate_db

    def set_login(self, host, port, user, password=None):
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
        self.login1 = Login(host, port, user, password, db=self.destination_db)

    def execute_query(self, sql):
        """
        establishes a connection and executes some code
        """
        with Connection(login=self.login0) as conn0, Connection(login=self.login1) as conn1:
            self.run_query(sql)

    def extract(self):
        self.set_pg_path()
        if self.recreate_db:
            self.create_target_db(self.login1)
        with Connection(login=self.login0) as conn0, Connection(login=self.login1) as conn1:
            self.conn0 = conn0
            self.conn1 = conn1
            self.set_session_authorization(self.conn0)
            self.set_search_path('conn0')
            self.create_temp_schema()
            self.create_target_boundary()
            for tn, geom in self.tables.iteritems():
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
        self.cluster_and_analyse()

        self.further_stuff()

    def further_stuff(self):
        """
        To be defined in the subclass
        """

    def additional_stuff(self):
        """
        additional steps, to be defined in the subclass
        """

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
        sql = """
SELECT t.*
INTO {temp}.{tn}
FROM {schema}.{tn} t, {temp}.boundary tb
WHERE
t.{geom} && tb.geom
        """
        self.run_query(sql.format(tn=tn, temp=self.temp, geom=geom,
                                  schema=self.schema),
                       conn=self.conn0)

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

            sql = """
        SELECT
            st_srid(a.geom) AS srid
        FROM meta.boundary a;
        """
            cur.execute(sql)
            row = cur.fetchone()

            self.target_srid = row.srid

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
            self.PGPATH = '/usr/lib/postgresql/9.3/bin'
            self.SHELL = True

    def create_target_db(self, login):
        """
        create the target database
        """
        createdb = os.path.join(self.PGPATH, 'createdb')
        dropdb = os.path.join(self.PGPATH, 'dropdb')

        cmd = '''"{dropdb}" -U {user} -h {host} -p {port} -w {destination_db}'''.format(dropdb=dropdb, destination_db=login.db, user=login.user,
           port=login.port, host=login.host)
        logger.info(cmd)
        ret = subprocess.call(cmd, shell=self.SHELL)

        cmd = '''"{createdb}" -U {user} -h {host} -p {port} -w -T pg21_template {destination_db}'''.format(createdb=createdb, destination_db=login.db, user=login.user,
           port=login.port, host=login.host)
        logger.info(cmd)
        ret = subprocess.call(cmd, shell=self.SHELL)
        if ret:
            raise IOError('Database {db} could not be recreated'.format(db=login.db))

        sql = """
ALTER DATABASE {db} OWNER TO {role};
        """
        with Connection(login=self.login0) as conn0:
            self.run_query(sql.format(db=login.db,
                                      role=self.role),
                           conn=conn0)
            conn0.commit()


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
            #'--clean',
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
        sql = '''DROP SCHEMA IF EXISTS {temp} CASCADE'''.format(temp=self.temp)
        self.run_query(sql, conn=self.conn0)

    def cluster_and_analyse(self):
        """
        """
        login = self.login1

        clusterdb = os.path.join(self.PGPATH, 'clusterdb')
        cmd = '''"{clusterdb}" -U {user} -h {host} -p {port} -w --verbose -d {destination_db}'''.format(clusterdb=clusterdb, destination_db=login.db, user=login.user,
           port=login.port, host=login.host)
        logger.info(cmd)
        subprocess.call(cmd, shell=self.SHELL)

        vacuumdb = os.path.join(self.PGPATH, 'vacuumdb')
        cmd = '''"{vacuumdb}" -U {user} -h {host} -p {port} -w --analyze -d {destination_db}'''.format(vacuumdb=vacuumdb, destination_db=login.db, user=login.user,
           port=login.port, host=login.host)
        logger.info(cmd)
        subprocess.call(cmd, shell=self.SHELL)


class ExtractMeta(Extract):
    """
    Create the target DB and Extract the Meta Tables
    """
    schema = 'meta'

    def additional_stuff(self):
        """
        additional steps, to be defined in the subclass
        """
        cursor = self.conn0.cursor()

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
  AND NOT s.finished;
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
  IF NOT NEW.todo AND NOT NEW.finished
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
CREATE OR REPLACE FUNCTION {temp}.check_dependency_order()
  RETURNS trigger AS
$BODY$DECLARE
id_sc INTEGER;
id_needs_script INTEGER;
BEGIN
  SELECT id INTO id_sc FROM meta.scripts s
  WHERE NEW.scriptcode = s.scriptcode;
  SELECT id INTO id_needs_script FROM meta.scripts s
  WHERE NEW.needs_script = s.scriptcode;
  IF NOT id_sc > id_needs_script
  THEN RAISE EXCEPTION
  'the id of % (%) has to be lower than the id of % (%)',
  NEW.needs_script, id_needs_script, NEW.scriptcode, id_sc;
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
(
  id integer NOT NULL,
  scriptcode text,
  scriptname text,
  desctiption text,
  parameter text,
  started boolean NOT NULL DEFAULT false,
  finished boolean NOT NULL DEFAULT false,
  starttime timestamp with time zone,
  endtime timestamp with time zone,
  todo boolean NOT NULL DEFAULT false,
  category text,
  CONSTRAINT scripts_pkey PRIMARY KEY (id),
  CONSTRAINT scripts_scriptcode_key UNIQUE (scriptcode)
);
CREATE TABLE {temp}.dependencies
(
  scriptcode text NOT NULL,
  needs_script text NOT NULL,
  CONSTRAINT dependencies_pkey PRIMARY KEY (scriptcode, needs_script),
  CONSTRAINT dependencies_needs_script_fkey FOREIGN KEY (needs_script)
      REFERENCES {temp}.scripts (scriptcode) MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE NO ACTION,
  CONSTRAINT dependencies_scriptname_fkey FOREIGN KEY (scriptcode)
      REFERENCES {temp}.scripts (scriptcode) MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE NO ACTION
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

CREATE CONSTRAINT TRIGGER check_dependency_order_trigger
  AFTER INSERT OR UPDATE
  ON {temp}.dependencies
  FOR EACH ROW
  EXECUTE PROCEDURE {temp}.check_dependency_order();

'''.format(schema=self.schema, temp=self.temp)
        self.run_query(sql, conn=self.conn0)

        sql = '''INSERT INTO {temp}.scripts
SELECT * FROM {schema}.scripts;

INSERT INTO {temp}.dependencies
SELECT * FROM {schema}.dependencies;
'''.format(schema=self.schema, temp=self.temp)
        self.run_query(sql, conn=self.conn0)

        sql = '''
UPDATE {temp}.scripts
SET started=False, finished=False, todo=False;
        '''.format(temp=self.temp)
        cursor.execute(sql)


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
                        dest="host", default='gis.ggr-planung.de')
    parser.add_argument("-p", '--port', action="store",
                        help="port", type=int,
                        dest="port", default=5432)

    parser.add_argument("-U", '--user', action="store",
                        help="database user",
                        dest="user", default='max')
    parser.add_argument('--source-db', action="store",
                        help="source database",
                        dest="source_db", default='dplus')

    options = parser.parse_args()

    bbox = BBox(top=options.top, bottom=options.bottom,
                left=options.left, right=options.right)
    extract = ExtractOSM(source_db=options.source_db,
                         destination_db=options.destination_db,
                         target_srid=options.srid,
                         recreate_db=True)
    extract.set_login(host=options.host, port=options.port, user=options.user)
    extract.get_target_boundary(bbox)
    extract.extract()

    extract = ExtractLanduse(source_db=options.source_db,
                             destination_db=options.destination_db,
                             target_srid=options.srid,
                             recreate_db=False)
    extract.set_login(host=options.host, port=options.port, user=options.user)
    extract.get_target_boundary(bbox)
    extract.extract()
