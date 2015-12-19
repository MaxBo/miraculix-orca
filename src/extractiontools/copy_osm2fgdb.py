#!/usr/bin/env python
#coding:utf-8

from argparse import ArgumentParser

import logging
logger = logging.getLogger()
logger.addHandler(logging.StreamHandler())
logger.level = logging.DEBUG

from extractiontools.connection import Connection
from extractiontools.copy2fgdb import Copy2FGDB

class CopyOSM2FGDB(Copy2FGDB):

    def create_views(self):
        """Create the osm views that should be exported"""
        with Connection(login=self.login1):
            self.create_railways()
            self.create_leisure()
            self.create_natural()
            self.create_waterway()
            self.create_buildings()

    def create_point_layer(self, key, view, schema='osm'):
        """Create a point layer schema.view, given by key"""
        sql = """
CREATE OR REPLACE VIEW {schema}.{view} AS
 SELECT w.id,
    n.tags -> '{key}'::text AS val,
    n.tags -> 'name'::text AS name,
    n.geom AS geom
   FROM osm.nodes n
  WHERE n.tags ? '{key}'::text;
"""
        self.run_query(sql.format(schema=schema,
                                  view=view,
                                  key=key))


    def create_poly_and_multipolygons(self, schema='osm'):
        """

        """
        sql = """
-- -- -- -- -- --
--  POLYGONS - --
-- -- -- -- -- --
-- CREATE TABLE WITH POLYGONS MADE OF A SINGLE LINESTRING
DROP TABLE osm.simple_polys;
CREATE TABLE osm.simple_polys
(id bigint PRIMARY KEY, geom geometry(POLYGON, {srid}));

INSERT INTO osm.simple_polys
SELECT w.id,
    ST_MakePolygon(w.linestring) as geom
  FROM osm.ways w
  WHERE st_IsClosed(w.linestring)
    and st_NPoints(w.linestring) > 3
;

-- -- -- -- -- --
--  MULTIPOLYGONS - --
-- -- -- -- -- --
DROP TABLE IF EXISTS osm.multipolygons;
CREATE TABLE osm.multipolygons (
relation_id bigint PRIMARY KEY,
tags hstore,
outerring_linestring geometry,
outerring_array bigint[],
outerring geometry,
innerring_linestring geometry[],
polygon geometry,
bbox geometry(POLYGON, {srid}),
poly_type text NOT NULL DEFAULT 'unknown');

INSERT INTO osm.multipolygons (relation_id, tags)
SELECT r.id, r.tags
FROM osm.relations r
WHERE r.tags -> 'type' = 'multipolygon'
AND r.tags ? 'type';

UPDATE osm.multipolygons r
SET outerring_array = a.arr,
outerring_linestring = a.geom
FROM (
SELECT r.relation_id,
array_agg(rm.member_id) as arr,
ST_LineMerge(ST_Collect(w.linestring)) AS geom
  FROM
  osm.multipolygons r,
  osm.relation_members rm,
  osm.ways w
  WHERE rm.member_role = 'outer'
    and rm.relation_id = r.relation_id
    and w.id = rm.member_id
    and st_NPoints(w.linestring) > 1
    and st_IsValid(w.linestring)
  GROUP BY r.relation_id) a
  WHERE a.relation_id = r.relation_id;

UPDATE osm.multipolygons r
SET innerring_linestring = b.geom_arr
FROM (
SELECT a.relation_id,
array_agg(geom) AS geom_arr
FROM (
SELECT
rr.relation_id,
(st_dump(ST_LineMerge(ST_Collect(w.linestring)))).geom
  FROM
  osm.relation_members rm,
  osm.ways w,
      osm.multipolygons rr
  WHERE rm.member_role = 'inner'
    and rm.relation_id = rr.relation_id
    and w.id = rm.member_id
  GROUP BY rr.relation_id ) a
  GROUP BY a.relation_id ) b
  WHERE b.relation_id = r.relation_id;


SELECT relation_id, array_length(innerring_linestring, 1) FROM osm.multipolygons;

-- a ring with only 3 points is flat: A-B-A (1st point = 3rd point), hence buggy
UPDATE osm.multipolygons r
SET poly_type= 'no valid outerring' WHERE
st_NPoints(r.outerring_linestring) < 4 -- 5 relations are buggy in italy.osm
or r.outerring_linestring IS NULL -- about 16000 (relations between simple nodes?)
;
-- the above must be done before what follows, because if less than 3 points, test may crash
UPDATE osm.multipolygons r
SET poly_type= 'no valid outerring' WHERE
r.poly_type = 'unknown'
and NOT st_IsClosed(r.outerring_linestring); -- 136 are buggy in italy.osm

UPDATE osm.multipolygons r
SET poly_type= 'no valid outerring' WHERE
r.poly_type = 'unknown'
and NOT st_IsSimple(r.outerring_linestring); -- 102 more are buggy in italy.osm


-- If (NOT poly_type= 'no valid outerring') after the above,
-- it means there is a valid outerring. Now, l
-- et us see if there is a valid innerring (or several)

-- if there is no inner line, there is no valid innerring
UPDATE osm.multipolygons r
SET poly_type= 'no valid innerring'
WHERE r.poly_type = 'unknown'
and r.innerring_linestring IS NULL
; -- 3015 more have no valid innerring

-- innering must be closed
UPDATE osm.multipolygons r
SET poly_type= 'no valid innerring'
WHERE r.poly_type = 'unknown'
and (NOT st_ISClosed(ST_LineMerge(ST_Collect(r.innerring_linestring))))
; -- 44 more are buggy

-- innering must be big enough
-- all innerrings must have at least three points
UPDATE osm.multipolygons r
SET poly_type= 'no valid innerring'
FROM (
SELECT r2.relation_id
FROM (
SELECT r.relation_id, st_NPoints(unnest(r.innerring_linestring)) n
FROM osm.multipolygons r ) r2
GROUP BY r2.relation_id
HAVING not bool_and(r2.n > 3)) r3
WHERE r.poly_type = 'unknown'
AND r3.relation_id = r.relation_id
;

-- check further validity of innerring: closed (multi)linestring?
UPDATE osm.multipolygons r
SET poly_type= 'valid innerring'
WHERE r.poly_type = 'unknown'
and (st_ISClosed(ST_LineMerge(ST_Collect(r.innerring_linestring))))
;

--finally create the multipolygon
UPDATE osm.multipolygons r
SET polygon = ST_MakePolygon(r.outerring_linestring, (r.innerring_linestring ))
WHERE poly_type= 'valid innerring'
and GeometryType(r.outerring_linestring) ='LINESTRING';



-- the complex polygons that are valid no longer need to be represented with their outerring only
-- and not deleting those simple_polys will prevent insertion in the final polygon UNION below
DELETE FROM osm.simple_polys p WHERE p.id IN (
  SELECT rm.member_id
  FROM osm.relation_members rm
  WHERE rm.member_role='outer'
  and rm.relation_id
  IN
   (SELECT r.relation_id
    FROM osm.multipolygons r
    WHERE r.poly_type='valid innerring'
    and ST_IsValid(r.polygon)
)
);

-- Also clean useless innerrings stored as simple_polys or ways
DELETE FROM osm.simple_polys p WHERE p.id IN (
  SELECT rm.member_id
  FROM osm.relation_members rm
  WHERE rm.member_role='inner'
  and rm.relation_id
  IN
   (SELECT r.relation_id
    FROM osm.multipolygons r
    WHERE r.poly_type='valid innerring'
    and ST_IsValid(r.polygon)
)
);

-- Create simple polygon not having (valid) innering(s)
UPDATE osm.multipolygons r
SET polygon = st_MakePolygon(r.outerring_linestring)
WHERE (r.poly_type = 'no valid innerring'
OR r.poly_type = 'unknown')
and GeometryType((r.outerring_linestring)) ='LINESTRING'
;

-- UPDATE tags of the multipolygon with the tags from the polygon
UPDATE osm.multipolygons r
SET tags = w2.tags || r.tags
FROM
(SELECT
wr.relation_id,
hstore_sum(w.tags) AS tags
FROM
osm.ways w,
(SELECT
r.relation_id,
unnest(r.outerring_array) AS wayid
FROM osm.multipolygons r
WHERE r.poly_type= 'valid innerring'
) wr
WHERE w.id = wr.wayid
GROUP BY wr.relation_id ) w2
WHERE r.relation_id = w2.relation_id
AND w2.tags != ''
;

-- set the bbox for spatial queries
UPDATE osm.multipolygons
SET bbox = ST_Envelope(polygon);

CREATE OR REPLACE VIEW osm.polygons
AS
SELECT
p.id,
p.geom,
w.bbox,
w.tags
FROM osm.simple_polys p, osm.ways w
WHERE p.id = w.id

UNION ALL

SELECT m.relation_id AS id,
m.polygon AS geom,
m.bbox,
m.tags
FROM osm.multipolygons m
WHERE m.poly_type != 'no valid outerring';

CREATE INDEX poly_tags_idx ON osm.multipolygons
USING gist(tags);
CREATE INDEX poly_geom_idx ON osm.multipolygons
USING gist(bbox);

        """
        self.run_query(sql, srid=self.options.target_srid)


    def create_multipolygon_layer(self,
                                  fields,
                                  where_clause,
                                  view,
                                  schema='osm'):
        """Create a multipolygon layer schema.view, with the given fields
        and the given where-clause"""
        sql = """


"""
        self.run_query(sql.format(schema=schema,
                                  view=view,
                                  key=key))

    def create_railways(self):
        """Create railways layer"""
        sql = """
CREATE OR REPLACE VIEW osm.buildings AS
 SELECT w.id,
    w.tags -> 'building'::text AS val,
    w.tags -> 'name'::text AS name,
    st_makepolygon(w.linestring)::geometry(Polygon,3044) AS geom
   FROM osm.ways w
  WHERE w.tags ? 'building'::text AND st_isclosed(w.linestring);
"""



if __name__ == '__main__':

    parser = ArgumentParser(description="Copy Data to File Geodatabase")

    parser.add_argument("-n", '--name', action="store",
                        help="Name of destination database",
                        dest="destination_db", default='extract')

    parser.add_argument("-s", '--srid', action="store",
                        help="srid of the target database", type=int,
                        dest="target_srid")

    parser.add_argument('--host', action="store",
                        help="host",
                        dest="host", default='gis.ggr-planung.de')
    parser.add_argument("-p", '--port', action="store",
                        help="port", type=int,
                        dest="port", default=5432)

    parser.add_argument("-U", '--user', action="store",
                        help="database user",
                        dest="user", default='osm')
    parser.add_argument('--schema', action="store",
                        help="schema",
                        dest="schema", default='network_car')
    parser.add_argument('--destschema', action="store",
                        help="destination schema in the FileGDB",
                        dest="dest_schema", default='network_car')
    parser.add_argument('--gdbname', action="store",
                        help="Name of the FileGDB to create",
                        dest="gdbname")
    parser.add_argument('--layers', action='store',
                        help='layers to copy,',
                        dest='layers',
                        nargs='+',
                        default=['autobahn',
                                 'hauptstr',
                                 'nebennetz',
                                 'faehren',
                                 'unaccessible_links',
                                 ])

    options = parser.parse_args()
    copy2fgdb = CopyOSM2FGDB(options)
    copy2fgdb.create_views()
    copy2fgdb.copy_layers()
