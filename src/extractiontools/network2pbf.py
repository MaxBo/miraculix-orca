#!/usr/bin/env python
#coding:utf-8

from argparse import ArgumentParser

import logging
logger = logging.getLogger()
logger.addHandler(logging.StreamHandler())
logger.level = logging.DEBUG
import sys
import os
import subprocess
import psycopg2
from extractiontools.connection import Login, Connection, DBApp


class CopyNetwork2Pbf(DBApp):
    """
    Copy osm data that belong to a network
    """
    def __init__(self, options):

        """"""
        self.options = options
        self.check_platform()
        self.login = Login(self.options.host,
                           self.options.port,
                           self.options.user,
                           db=self.options.destination_db)

    def copy(self):
        """
        main program
        """
        with Connection(login=self.login) as conn:
            # preparation
            self.conn = conn
            self.set_session_authorization(self.conn)
            self.create_views()
            self.conn.commit()
            self.reset_authorization(self.conn)
        self.copy2pbf()

    def create_views(self):
        """"""
        sql = """
DROP SCHEMA IF EXISTS {schema} CASCADE;
CREATE SCHEMA {schema};

CREATE OR REPLACE VIEW {schema}.actions AS
 SELECT a.data_type,
    a.action,
    a.id
   FROM osm.actions a;

CREATE OR REPLACE VIEW {schema}.boundary AS
 SELECT b.id,
    st_transform(b.geom, {srid}) AS geom
   FROM meta.boundary b;

DROP VIEW IF EXISTS {schema}.ways CASCADE;
CREATE MATERIALIZED VIEW {schema}.ways AS
 SELECT w.id,
    w.version,
    w.user_id,
    w.tstamp,
    w.changeset_id,
    w.tags,
    w.nodes,
    st_transform(w.bbox, {srid}) AS bbox,
    st_transform(w.linestring, {srid}) AS linestring
   FROM osm.ways w, {network}.links_reached_without_planned l
--   FROM osm.ways w, {network}.links l
   WHERE w.id = l.wayid;
CREATE INDEX way_id_idx ON {schema}.ways
USING btree(id);

CREATE OR REPLACE VIEW {schema}.schema_info AS
 SELECT s.version
   FROM osm.schema_info s;

CREATE OR REPLACE VIEW {schema}.way_nodes AS
 SELECT wn.way_id,
    wn.node_id,
    wn.sequence_id
   FROM osm.way_nodes wn, {schema}.ways w
   WHERE wn.way_id = w.id;

DROP VIEW IF EXISTS {schema}.nodes CASCADE;
CREATE MATERIALIZED VIEW {schema}.nodes AS
 SELECT n.id,
    n.version,
    n.user_id,
    n.tstamp,
    n.changeset_id,
    n.tags,
    st_transform(n.geom, {srid}) AS geom
   FROM osm.nodes n, (SELECT DISTINCT node_id FROM {schema}.way_nodes) wn
   WHERE n.id = wn.node_id;
CREATE INDEX node_id_idx ON {schema}.nodes
USING btree(id);

CREATE TABLE {schema}.active_relations (id integer primary key);

INSERT INTO {schema}.active_relations
SELECT DISTINCT r.id
FROM osm.relations r,
osm.relation_members rm,
{schema}.nodes n
WHERE r.id = rm.relation_id
AND rm.member_type = 'N'
AND rm.member_id = n.id;

INSERT INTO {schema}.active_relations
SELECT DISTINCT r.id
FROM osm.relations r,
osm.relation_members rm,
{schema}.ways w
WHERE r.id = rm.relation_id
AND rm.member_type = 'W'
AND rm.member_id = w.id
AND NOT EXISTS (SELECT 1 FROM {schema}.active_relations ar WHERE r.id = ar.id);

INSERT INTO {schema}.active_relations
SELECT DISTINCT r.id
FROM osm.relations r,
osm.relation_members rm,
{schema}.active_relations ar
WHERE r.id = rm.relation_id
AND rm.member_type = 'R'
AND rm.member_id = ar.id
AND NOT EXISTS (SELECT 1 FROM {schema}.active_relations ar WHERE r.id = ar.id);

CREATE OR REPLACE VIEW {schema}.relations AS
 SELECT r.id,
    r.version,
    r.user_id,
    r.tstamp,
    r.changeset_id,
    r.tags
   FROM osm.relations r,
   {schema}.active_relations ar
   WHERE r.id = ar.id;

CREATE OR REPLACE VIEW {schema}.relation_members AS
 SELECT rm.relation_id,
    rm.member_id,
    rm.member_type,
    rm.member_role,
    rm.sequence_id
   FROM osm.relation_members rm,
   {schema}.active_relations ar
   WHERE rm.relation_id = ar.id;

CREATE OR REPLACE VIEW {schema}.users AS
 SELECT u.id,
    u.name
   FROM osm.users u
   WHERE EXISTS
   (SELECT 1 FROM {schema}.relations r WHERE u.id = r.user_id)
   OR EXISTS
   (SELECT 1 FROM {schema}.ways w WHERE u.id = w.user_id)
   OR EXISTS
   (SELECT 1 FROM {schema}.nodes n WHERE u.id = n.user_id);

        """.format(schema=self.options.user,
                   network=self.options.network,
                   srid=self.options.srid)
        self.run_query(sql)

    def check_platform(self):
        """
        check the platform
        """
        if sys.platform.startswith('win'):
            self.OSM_FOLDER = r'E:\Modell\osmosis'
            self.OSMOSISPATH = os.path.join(self.OSM_FOLDER, 'bin',
                                            'osmosis.bat')
            self.AUTHFILE = os.path.join(self.OSM_FOLDER, 'config', 'pwd')
            self.folder = r'C:\temp'
            self.SHELL = False
        else:
            self.OSM_FOLDER = '~/gis/osm'
            self.OSMOSISPATH = os.path.join(self.OSM_FOLDER, 'osmosis',
                                            'bin', 'osmosis')
            self.AUTHFILE = os.path.join(self.OSM_FOLDER, 'config', 'pwd')
            self.folder = '~/gis'
            self.SHELL = True

    def copy2pbf(self):
        """
        copy the according schema to a pbf with osmosis
        """

        fn = '{db}_{network}'.format(db=self.options.destination_db,
                                         network=self.options.network)
        folder = os.path.join(self.folder,
                              'projekte',
                              self.options.destination_db,
                              'pbf', )
        ret = subprocess.call('mkdir -p {}'.format(folder), shell=self.SHELL)

        file_path = os.path.join(folder, fn)

        if self.options.xml:
            to_xml = ' --tee --write-xml file={xml_file}.osm.gz '.format(xml_file=file_path)
        else:
            to_xml = ''
        cmd = '{OSMOSIS} -v --read-pgsql authFile="{authfile}" host={host}:{port} user={user} database={db} --dataset-dump {to_xml}--write-pbf file={fn}.pbf'

        full_cmd = cmd.format(OSMOSIS=self.OSMOSISPATH,
                              authfile=self.AUTHFILE,
                              host=self.options.host,
                              port=self.options.port,
                              user=self.options.user,
                              db=self.options.destination_db,
                              fn=file_path,
                              to_xml=to_xml,
                              )
        logger.info(full_cmd)
        ret = subprocess.call(full_cmd, shell=self.SHELL)
        if ret:
            raise IOError('Layer {layer} could copied to Pbf'.format(layer=layer))

if __name__ == '__main__':

    parser = ArgumentParser(description="Copy Data to File Geodatabase")

    parser.add_argument("-n", '--name', action="store",
                        help="Name of destination database",
                        dest="destination_db", default='extract')

    parser.add_argument('--host', action="store",
                        help="host",
                        dest="host", default='gis.ggr-planung.de')
    parser.add_argument("-p", '--port', action="store",
                        help="port", type=int,
                        dest="port", default=5432)

    parser.add_argument("-U", '--user', action="store",
                        help="database user",
                        dest="user", default='osm84')

    parser.add_argument('--network', action="store",
                        help="network",
                        dest="network", default='network_fr')

    parser.add_argument("-s", '--srid', action="store",
                        help="srid of the target pbf", type=int,
                        dest="srid", default='4326')

    parser.add_argument('--xml', action="store_true",
                        help="also export as xml",
                        dest="xml", default='False')

    options = parser.parse_args()

    copy2pbf = CopyNetwork2Pbf(options)
    copy2pbf.copy()
