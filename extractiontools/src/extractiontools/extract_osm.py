#!/usr/bin/env python
#coding:utf-8

from argparse import ArgumentParser

from extractiontools.ausschnitt import Extract, BBox, logger


class ExtractOSM(Extract):
    """
    Extract the osm data
    """
    schema = 'osm'
    role = 'group_osm'

    def copy_relations(self):
        """
        copy relation and relation_member Schema
        """
        sql = """
        SELECT *
        INTO {schema}.relations
        FROM {temp}.relations
        LIMIT 0;

        SELECT *
        INTO {schema}.relation_members
        FROM {temp}.relation_members
        LIMIT 0;
        """.format(temp=self.temp, schema=self.schema)
        self.run_query(sql, conn=self.conn)
        self.conn.commit()

        sql = """
-- copy relations for ways and nodes
INSERT INTO {schema}.relations
SELECT DISTINCT ON (r.id) r.*
FROM {temp}.relations r,
(SELECT rm.relation_id AS rid
FROM {temp}.relation_members rm, {schema}.ways AS w
WHERE rm.member_type = 'W'
AND w.id = rm.member_id

UNION ALL

SELECT rm.relation_id AS rid
FROM {temp}.relation_members rm, {schema}.nodes AS n
WHERE rm.member_type = 'N'
AND n.id = rm.member_id) rnw

WHERE rnw.rid = r.id
;
        """.format(temp=self.temp, schema=self.schema)
        self.run_query(sql, conn=self.conn)

        sql = """
-- Insert relation that point to an existing relation
INSERT INTO {schema}.relations
SELECT DISTINCT ON (r.id) r.*
FROM {temp}.relations r, {temp}.relation_members rm, {schema}.relations tr
WHERE r.id = rm.relation_id
AND rm.member_id = tr.id
AND rm.member_type = 'R'
AND NOT EXISTS (SELECT 1 FROM {schema}.relations tr2 WHERE tr2.id = r.id);
        """.format(temp=self.temp, schema=self.schema)

        cur = self.conn.cursor()
        n_inserted = 1
        while n_inserted:
            logger.info(sql)
            cur.execute(sql)
            msg = cur.statusmessage
            n_inserted = int(msg.split(' ')[1])
            logger.info('INSERT relation of relations: %s' % cur.statusmessage)

        sql = """
-- INSERT Relation members
INSERT INTO {schema}.relation_members
SELECT rm.*
FROM {temp}.relation_members rm, {schema}.relations r
WHERE r.id = rm.relation_id;
        """.format(temp=self.temp, schema=self.schema)
        self.run_query(sql, conn=self.conn)


    def copy_way_nodes(self):
        """
        copy the way_nodes in that area
        """

        sql = """
-- copy way nodes
SELECT wn.*
INTO {schema}.way_nodes
FROM {temp}.way_nodes wn, {schema}.ways w
WHERE wn.way_id = w.id;

-- copy nodes that are in way_nodes but not yet in nodes
INSERT INTO {schema}.nodes
SELECT DISTINCT ON (n.id)
  n.id, n.version, n.user_id, n.tstamp, n.changeset_id, n.tags,
  st_transform(n.geom, {target_srid}) AS geom
FROM {temp}.nodes n, {schema}.way_nodes wn
WHERE n.id = wn.node_id
AND NOT EXISTS (SELECT 1 FROM {schema}.nodes tn WHERE tn.id= n.id);
        """.format(temp=self.temp, schema=self.schema,
                   target_srid=self.target_srid)

        self.run_query(sql, conn=self.conn)

    def copy_users(self):
        """
        copy users
        """
        sql = """
SELECT DISTINCT ON (u.id) u.*
INTO {schema}.users
FROM {temp}.users u,
(SELECT DISTINCT user_id FROM {schema}.nodes
UNION ALL
SELECT DISTINCT user_id FROM {schema}.ways
UNION ALL
SELECT DISTINCT user_id FROM {schema}.relations) tu
WHERE u.id = tu.user_id;
        """.format(temp=self.temp, schema=self.schema)
        self.run_query(sql, conn=self.conn)


    def additional_stuff(self):
        """
        """
        self.extract_nodes()
        self.extract_ways()
        self.copy_relations()
        self.copy_way_nodes()
        self.copy_users()
        self.copy_schema_info()

    def extract_ways(self):
        """
        """
        sql = """
SELECT
  w.id, w.version, w.user_id, w.tstamp, w.changeset_id, w.tags, w.nodes,
  st_transform(st_setsrid(Box2D(w.linestring), {source_srid}), {target_srid})::geometry('GEOMETRY', {target_srid}) AS bbox,
  st_transform(w.linestring, {target_srid})::geometry('GEOMETRY', {target_srid}) AS linestring
INTO {schema}.ways
FROM {temp}.ways w, {schema}.boundary tb
WHERE
st_intersects(w.linestring, tb.source_geom);
ANALYZE {schema}.ways;
        """
        self.run_query(sql.format(temp=self.temp, schema=self.schema,
                                  target_srid=self.target_srid,
                                  source_srid=self.srid),
                       conn=self.conn)

    def extract_nodes(self):
        """
        """
        sql = """
SELECT
  n.id, n.version, n.user_id, n.tstamp, n.changeset_id, n.tags,
  st_transform(n.geom, {target_srid})::geometry('POINT', {target_srid}) AS geom
INTO {schema}.nodes
FROM {temp}.nodes n, {schema}.boundary tb
WHERE
n.geom && tb.source_geom;
ANALYZE {schema}.nodes;
        """
        self.run_query(sql.format(temp=self.temp, schema=self.schema,
                                  target_srid=self.target_srid),
                       conn=self.conn)


    def copy_schema_info(self):
        """
        copy schema and actions
        """
        sql = """
SELECT a.*
INTO {schema}.actions
FROM {temp}.actions a;

SELECT s.*
INTO {schema}.schema_info
FROM {temp}.schema_info s;
        """.format(temp=self.temp, schema=self.schema)
        self.run_query(sql, self.conn)

    def create_index(self):
        """
        CREATE INDEX
        """
        sql = """
ALTER TABLE {schema}.actions ADD PRIMARY KEY (data_type, id);
ALTER TABLE {schema}.nodes ADD PRIMARY KEY (id);
CREATE INDEX idx_nodes_geom
  ON {schema}.nodes
  USING gist
  (geom);
ALTER TABLE {schema}.nodes CLUSTER ON idx_nodes_geom;
ALTER TABLE {schema}.relation_members ADD PRIMARY KEY (relation_id, sequence_id);
CREATE INDEX idx_relation_members_member_id_and_type
  ON {schema}.relation_members
  USING btree
  (member_id, member_type COLLATE pg_catalog."default");
ALTER TABLE {schema}.relations ADD PRIMARY KEY (id);
ALTER TABLE {schema}.schema_info ADD PRIMARY KEY (version);
ALTER TABLE {schema}.users ADD PRIMARY KEY (id);
ALTER TABLE {schema}.way_nodes ADD PRIMARY KEY (way_id, sequence_id);
CREATE INDEX idx_way_nodes_node_id
  ON {schema}.way_nodes
  USING btree
  (node_id);
ALTER TABLE {schema}.ways ADD PRIMARY KEY (id);
CREATE INDEX idx_ways_bbox
  ON {schema}.ways
  USING gist
  (bbox);

CREATE INDEX idx_ways_linestring
  ON {schema}.ways
  USING gist
  (linestring);
ALTER TABLE {schema}.ways CLUSTER ON idx_ways_linestring;

CREATE INDEX way_tags_idx
ON osm.ways
USING gist(tags);

-- Partial index for nodes
CREATE INDEX node_tags_idx
ON osm.nodes
USING gist(tags)
WHERE tags <> ''::hstore;

CREATE INDEX relations_tags_idx
ON osm.relations
USING gist(tags);

ALTER TABLE osm.way_nodes
   ALTER COLUMN way_id
   SET (n_distinct=-0.1);

ALTER TABLE osm.way_nodes
   ALTER COLUMN node_id
   SET (n_distinct=-0.75);

ALTER TABLE osm.nodes
   ALTER COLUMN tags
   SET (n_distinct=-0.1);

ALTER TABLE osm.ways
   ALTER COLUMN tags
   SET (n_distinct=-0.1);

ALTER TABLE osm.relations
   ALTER COLUMN tags
   SET (n_distinct=-0.1);


        """.format(schema=self.schema)
        self.run_query(sql, self.conn)
        self.tables2cluster.append('{schema}.nodes'.format(schema=self.schema))
        self.tables2cluster.append('{schema}.ways'.format(schema=self.schema))

    def further_stuff(self):
        """
        Copy the osm classifications and osm-view in wgs84
        to destination database
        """
        self.copy_schema_to_target_db(schema='classifications')
        self.copy_schema_to_target_db(schema='osm84')
        self.cluster_and_analyse()


if __name__ == '__main__':

    parser = ArgumentParser(description="Extract Data for Model")

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
                        dest="user", default='osm')
    parser.add_argument('--source-db', action="store",
                        help="source database",
                        dest="source_db", default='europe')

    options = parser.parse_args()

    extract = ExtractOSM(source_db=options.source_db,
                         destination_db=options.destination_db)
    extract.set_login(host=options.host,
                      port=options.port,
                      user=options.user)
    extract.get_target_boundary_from_dest_db()
    extract.extract()
