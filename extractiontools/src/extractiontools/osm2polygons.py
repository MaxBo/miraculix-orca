#!/usr/bin/env python
# coding:utf-8

from argparse import ArgumentParser

from extractiontools.connection import Connection, Login
from extractiontools.ausschnitt import Extract


class CreatePolygons(Extract):
    """"""

    def __init__(self, destination_db, **kwargs):
        """"""
        super().__init__(destination_db=destination_db, **kwargs)
        self.check_platform()

    def create_poly_and_multipolygons(self, schema='osm'):
        """
        Create Polygons and Multipolygons for OSM-Data
        """
        sql_create_simple_polygons = f"""
-- -- -- -- -- --
--  POLYGONS - --
-- -- -- -- -- --
-- CREATE TABLE WITH POLYGONS MADE OF A SINGLE LINESTRING
DROP TABLE IF EXISTS "{schema}".simple_polys CASCADE;
CREATE TABLE "{schema}".simple_polys
(id bigint PRIMARY KEY,
geom geometry(MULTIPOLYGON, {self.target_srid}));

INSERT INTO "{schema}".simple_polys
SELECT w.id,
    st_multi(ST_MakePolygon(w.linestring)) as geom
  FROM "{schema}".ways w
  WHERE st_IsClosed(w.linestring)
    and st_NPoints(w.linestring) > 3
;
"""
        sql_create_polygons_with_holes = f"""
-- -- -- -- -- --
--  POLYGONS WITH HOLES - --
-- -- -- -- -- --
DROP TABLE IF EXISTS "{schema}".polygon_with_holes CASCADE;
CREATE TABLE "{schema}".polygon_with_holes (
relation_id bigint PRIMARY KEY,
tags hstore,
outerring_linestring geometry(MULTILINESTRING, {self.target_srid}),
outerring_array bigint[],
innerring_linestring geometry[],
polygon geometry(MULTIPOLYGON, {self.target_srid}),
poly_type text NOT NULL DEFAULT 'unknown');

-- fill the table with all relations tagged "multipolygon"
INSERT INTO "{schema}".polygon_with_holes (relation_id, tags)
SELECT r.id, r.tags
FROM "{schema}".relations r
WHERE r.tags -> 'type' = 'multipolygon'
AND r.tags ? 'type';

-- set array of outerrings
UPDATE "{schema}".polygon_with_holes r
SET outerring_array = a.arr,
outerring_linestring = a.geom
FROM (
SELECT r.relation_id,
array_agg(rm.member_id) as arr,
st_multi(ST_LineMerge(ST_Collect(w.linestring))) AS geom
  FROM
  "{schema}".polygon_with_holes r,
  "{schema}".relation_members rm,
  "{schema}".ways w
  WHERE rm.member_role = 'outer'
    and rm.relation_id = r.relation_id
    and w.id = rm.member_id
    and st_NPoints(w.linestring) > 1
    and st_IsValid(w.linestring)
  GROUP BY r.relation_id) a
  WHERE a.relation_id = r.relation_id;

-- set innerrings
UPDATE "{schema}".polygon_with_holes r
SET innerring_linestring = b.geom_arr
FROM (
SELECT a.relation_id,
array_agg(geom) AS geom_arr
FROM (
SELECT
rr.relation_id,
(st_dump(ST_LineMerge(ST_Collect(w.linestring)))).geom
  FROM
    "{schema}".relation_members rm,
    "{schema}".ways w,
    "{schema}".polygon_with_holes rr
  WHERE rm.member_role = 'inner'
    and rm.relation_id = rr.relation_id
    and w.id = rm.member_id
  GROUP BY rr.relation_id ) a
  GROUP BY a.relation_id ) b
  WHERE b.relation_id = r.relation_id;

-- a ring with only 3 points is flat: A-B-A (1st point = 3rd point), hence buggy
UPDATE "{schema}".polygon_with_holes r
SET poly_type= 'no valid outerring' WHERE
st_NPoints(r.outerring_linestring) < 4 -- 5 relations are buggy in italy.osm
or r.outerring_linestring IS NULL -- about 16000 (relations between simple nodes?)
;
-- the above must be done before what follows, because if less than 3 points, test may crash
UPDATE "{schema}".polygon_with_holes r
SET poly_type= 'no valid outerring' WHERE
r.poly_type = 'unknown'
and NOT st_IsClosed(r.outerring_linestring); -- 136 are buggy in italy.osm

UPDATE "{schema}".polygon_with_holes r
SET poly_type= 'no valid outerring' WHERE
r.poly_type = 'unknown'
and NOT st_IsSimple(r.outerring_linestring); -- 102 more are buggy in italy.osm


-- If (NOT poly_type= 'no valid outerring') after the above,
-- it means there is a valid outerring. Now, l
-- et us see if there is a valid innerring (or several)

-- if there is no inner line, there is no valid innerring
UPDATE "{schema}".polygon_with_holes r
SET poly_type= 'no valid innerring'
WHERE r.poly_type = 'unknown'
and r.innerring_linestring IS NULL
; -- 3015 more have no valid innerring

-- innering must be closed
UPDATE "{schema}".polygon_with_holes r
SET poly_type= 'no valid innerring'
WHERE r.poly_type = 'unknown'
and (NOT st_ISClosed(ST_LineMerge(ST_Collect(r.innerring_linestring))))
; -- 44 more are buggy

-- innering must be big enough
-- all innerrings must have at least three points
UPDATE "{schema}".polygon_with_holes r
SET poly_type= 'no valid innerring'
FROM (
SELECT r2.relation_id
FROM (
SELECT r.relation_id, st_NPoints(unnest(r.innerring_linestring)) n
FROM "{schema}".polygon_with_holes r ) r2
GROUP BY r2.relation_id
HAVING not bool_and(r2.n > 3)) r3
WHERE r.poly_type = 'unknown'
AND r3.relation_id = r.relation_id
;

-- check further validity of innerring: closed (multi)linestring?
UPDATE "{schema}".polygon_with_holes r
SET poly_type= 'valid innerring'
WHERE r.poly_type = 'unknown'
and (st_ISClosed(ST_LineMerge(ST_Collect(r.innerring_linestring))))
;


--finally create the Polygon
UPDATE "{schema}".polygon_with_holes r
SET polygon = st_multi(ST_MakePolygon(r.outerring_linestring,
                                      (r.innerring_linestring )))
WHERE poly_type= 'valid innerring'
and GeometryType(r.outerring_linestring) ='LINESTRING';
"""

        sql_create_multipolygons = f"""
-- MULTIPOLYGONS
DROP TABLE IF EXISTS "{schema}".multi_polygons;
CREATE TABLE "{schema}".multi_polygons
(relation_id bigint,
path integer,
outerring geometry(LINESTRING, {self.target_srid}),
polygon geometry(POLYGON, {self.target_srid}));

ALTER TABLE "{schema}".multi_polygons
ADD PRIMARY KEY (relation_id, path);

-- fill the multipolygon_table
INSERT INTO "{schema}".multi_polygons (relation_id, path, outerring)
SELECT r.relation_id,
(st_dump(r.outerring_linestring)).path[1],
(st_dump(r.outerring_linestring)).geom
FROM "{schema}".polygon_with_holes r
WHERE geometrytype(r.outerring_linestring) = 'MULTILINESTRING';

-- build the outerring polygon
UPDATE "{schema}".multi_polygons m
SET polygon = st_makepolygon(outerring)
WHERE st_isclosed(outerring)
AND st_npoints(outerring) > 3;

-- create the full polygon
UPDATE "{schema}".multi_polygons m
SET polygon = st_makepolygon(m.outerring, i2.geom_agg)
FROM
(SELECT
  i.relation_id,
  i.path,
  array_agg(i.geom) AS geom_agg
 FROM
(SELECT
  m.relation_id,
  m.path,
  m.polygon,
  -- one row per innerring
  unnest(r.innerring_linestring) AS geom
FROM
  "{schema}".polygon_with_holes r,
  "{schema}".multi_polygons m
WHERE r.relation_id = m.relation_id ) i
-- check which innerring is in which outer polygon for each relation
WHERE st_within(i.geom, i.polygon)
-- only consider valid innerrings
AND st_isclosed(i.geom)
AND st_npoints(i.geom) > 3

-- regroup the valid innerrings to a geometry array
GROUP BY i.relation_id, i.path) AS i2
WHERE m.relation_id = i2.relation_id
AND m.path = i2.path;


-- and the Multipolygons
UPDATE "{schema}".polygon_with_holes r
SET polygon = mm.geom,
poly_type = 'multipolygon'
FROM
(SELECT m.relation_id,
st_multi(st_collect(m.polygon)) AS geom
FROM
"{schema}".multi_polygons m,
"{schema}".polygon_with_holes r
WHERE m.relation_id = r.relation_id
GROUP BY m.relation_id) mm
WHERE mm.relation_id = r.relation_id;

"""
        sql_delete_unused_simple_polygons = f"""
-- the complex polygons that are valid no longer need to be represented with their outerring only
-- and not deleting those simple_polys will prevent insertion in the final polygon UNION below

CREATE TABLE IF NOT EXISTS "{schema}".ways_in_poly (id bigint PRIMARY KEY);
TRUNCATE "{schema}".ways_in_poly;
INSERT INTO "{schema}".ways_in_poly
SELECT DISTINCT rm.member_id
  FROM "{schema}".relation_members rm
  WHERE rm.member_role='outer'
  AND rm.member_type = 'W'
  and rm.relation_id
  IN
   (SELECT r.relation_id
    FROM "{schema}".polygon_with_holes r
    WHERE ST_IsValid(r.polygon))
    ;

DELETE FROM "{schema}".simple_polys p WHERE p.id IN (
  SELECT rm.member_id
  FROM "{schema}".relation_members rm
  WHERE rm.member_role='outer'
  and rm.relation_id
  IN
   (SELECT r.relation_id
    FROM "{schema}".polygon_with_holes r
    WHERE ST_IsValid(r.polygon)
)
);


-- Also clean useless innerrings stored as simple_polys or ways

INSERT INTO "{schema}".ways_in_poly
SELECT DISTINCT rm.member_id
  FROM "{schema}".relation_members rm,
  "{schema}".ways w
  WHERE
  rm.member_id = w.id
  AND rm.member_role='inner'
  AND rm.member_type = 'W'
  AND w.tags = ''
  and rm.relation_id
  IN
   (SELECT r.relation_id
    FROM "{schema}".polygon_with_holes r
    WHERE ST_IsValid(r.polygon))
AND NOT EXISTS (
SELECT 1 FROM "{schema}".ways_in_poly p
WHERE p.id = rm.member_id)
;

DELETE FROM "{schema}".simple_polys p WHERE p.id IN (
  SELECT DISTINCT rm.member_id
  FROM "{schema}".relation_members rm
  WHERE rm.member_role='inner'
  and rm.relation_id
  IN
   (SELECT r.relation_id
    FROM "{schema}".polygon_with_holes r
    WHERE ST_IsValid(r.polygon)
)
);

INSERT INTO "{schema}".ways_in_poly
SELECT s.id FROM "{schema}".simple_polys s
WHERE NOT EXISTS(
SELECT 1 FROM "{schema}".ways_in_poly p
WHERE p.id = s.id);

-- Create simple polygon not having (valid) innering(s)
UPDATE "{schema}".polygon_with_holes r
SET polygon = st_multi(st_MakePolygon(r.outerring_linestring))
WHERE (r.poly_type = 'no valid innerring'
OR r.poly_type = 'unknown')
and GeometryType((r.outerring_linestring)) ='LINESTRING'
;
"""
        sql_update_tags = f"""
-- UPDATE tags of the multipolygon with the tags from the polygon
UPDATE "{schema}".polygon_with_holes r
SET tags = w2.tags || r.tags
FROM
(SELECT
wr.relation_id,
hstore_sum(w.tags) AS tags
FROM
"{schema}".ways w,
(SELECT
r.relation_id,
unnest(r.outerring_array) AS wayid
FROM "{schema}".polygon_with_holes r
WHERE r.poly_type= 'valid innerring'
OR r.poly_type = 'multipolygon'
) wr
WHERE w.id = wr.wayid
GROUP BY wr.relation_id ) w2
WHERE r.relation_id = w2.relation_id
AND w2.tags != ''
;
"""
        sql_create_index = f"""
CREATE INDEX poly_tags_idx ON "{schema}".polygon_with_holes
USING gist(tags);
CREATE INDEX poly_geom_idx ON "{schema}".polygon_with_holes
USING gist(polygon);
CREATE INDEX simple_poly_geom_idx ON "{schema}".simple_polys
USING gist(geom);

ANALYZE "{schema}".polygon_with_holes;
ANALYZE "{schema}".simple_polys;

"""
        sql_create_view = f"""
CREATE OR REPLACE VIEW "{schema}".polygons
AS
SELECT
p.id,
p.geom,
w.tags
FROM "{schema}".simple_polys p, "{schema}".ways w
WHERE p.id = w.id

UNION ALL

SELECT m.relation_id AS id,
m.polygon AS geom,
m.tags
FROM "{schema}".polygon_with_holes m
WHERE m.poly_type != 'no valid outerring';

DROP VIEW IF EXISTS "{schema}".lines CASCADE;
CREATE OR REPLACE VIEW "{schema}".lines
AS
SELECT
  w.id,
  w.linestring::geometry(LINESTRING, {self.target_srid}) as geom,
  w.tags
FROM
  "{schema}".ways w
WHERE NOT EXISTS
  (SELECT 1 FROM "{schema}".ways_in_poly wp WHERE wp.id = w.id);
        """
        with Connection(login=self.login) as conn:
            self.conn = conn
            self.run_query(sql_create_simple_polygons)
            self.run_query(sql_create_polygons_with_holes)
            self.run_query(sql_create_multipolygons)
            self.run_query(sql_delete_unused_simple_polygons)
            self.run_query(sql_update_tags)
            self.run_query(sql_create_index)
            self.run_query(sql_create_view)
            self.conn.commit()


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
                        dest="host", default='localhost')
    parser.add_argument("-p", '--port', action="store",
                        help="port", type=int,
                        dest="port", default=5432)
    parser.add_argument("-U", '--user', action="store",
                        help="database user",
                        dest="user", default='osm')

    parser.add_argument('--schema', action="store",
                        help="schema",
                        dest="schema", default='osm')

    options = parser.parse_args()
    login = Login(**options.__dict__)
    copy2fgdb = CreatePolygons(login)
    copy2fgdb.get_target_boundary_from_dest_db()
    copy2fgdb.create_poly_and_multipolygons()
