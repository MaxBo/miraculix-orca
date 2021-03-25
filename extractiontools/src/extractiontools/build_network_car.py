#!/usr/bin/env python
# coding:utf-8

from argparse import ArgumentParser

from extractiontools.connection import Connection, DBApp
#from . import wingdbstub


class BuildNetwork(DBApp):
    """
    Build Network Analyst-Data in the target Database...
    """
    role = 'group_osm'

    def __init__(self,
                 schema='osm',
                 network_schema='network',
                 db='extract',
                 limit: int = None,
                 chunksize: int = 1000,
                 links_to_find: float = 0.25,
                 corine: str = 'clc18',
                 routing_walk=False,
                 **kwargs
                 ):
        super().__init__(**kwargs)

        self.db = db
        self.set_login(database=db)
        self.schema = schema
        self.network = network_schema
        self.limit = limit
        self.chunksize = chunksize
        self.links_to_find = links_to_find
        self.corine = corine
        self.routing_walk = False

    def build(self):
        """
        Build the network
        """
        self.logger.info(f'Build Network in schema {self.network}')
        with Connection(login=self.login) as conn:
            # preparation
            self.conn = conn
            self.get_srid()
            # self.set_session_authorization(self.conn)
            self.create_views()

            # select roads and junctions
            self.logger.info(f'Create Views')
            self.create_roads()
            self.create_junctions()
            self.conn.commit()
            # create functions
            self.create_functions()
            self.conn.commit()
            # create links
            self.fill_link_points_and_create_links()
            self.create_chunks()
            self.create_links()
            self.fill_links()
            self.conn.commit()
            # update link attributes
            self.update_linktypes()
            self.update_oneway()
            self.update_lanes()
            self.create_slope()
            self.update_speed()
            self.update_time()
            self.create_index()
            self.create_barriers()
            self.conn.commit()
            # prepare the search for accessible links
            self.create_pgrouting_network()
            self.conn.commit()
            self.update_egde_table()
            self.conn.commit()
            self.create_topology()
            self.create_edge_reached()
            self.conn.commit()
            # search accessible links including links reached by planned roads
            self.try_startvertices(n=20)
            self.copy_edge_reached_with_planned()
            # search links accessible only by existing roads
            self.update_edge_table_with_construction()
            self.try_startvertices(n=20)

            # create the final views
            self.create_view_accessible_links()
            self.create_views_roadtypes()
            self.conn.commit()
            self.reset_authorization(self.conn)

    def spatial_ref_systems(self):
        """
        Create spatial ref systems
        """
        sql = """
CREATE TABLE spatial_ref_name (name text PRIMARY KEY);
INSERT INTO spatial_ref_name (name) VALUES('gk3');
CREATE OR REPLACE VIEW refname AS
SELECT spatial_ref_names.id FROM spatial_ref_name,spatial_ref_names WHERE spatial_ref_name.name=spatial_ref_names.name;
"""

    def get_srid(self):
        """
        retrieve the SRID from meta.boundary
        """
        sql = """
SELECT st_srid(geom) AS srid FROM meta.boundary LIMIT 1;
        """
        cur = self.conn.cursor()
        cur.execute(sql)
        row = cur.fetchone()
        self.srid = row.srid

    def create_views(self):
        """
        Create Views defining the relevant waytypes
        """
        self.logger.info(f'Create Views')
        sql = """
DROP SCHEMA IF EXISTS "{network}" CASCADE;
CREATE SCHEMA "{network}";
        """.format(network=self.network)
        self.run_query(sql)

        sql = """
-- selektiere alle Wege und Straßen, die in waytype2linktype definiert sind
CREATE MATERIALIZED VIEW "{network}".streets AS
SELECT
  w.id,
  min(wtl.linktype_id) linktype_id,
  min(lt.road_category)::char(1) category
FROM
osm.ways w,
classifications.linktypes lt,
classifications.waytype2linktype wtl,
classifications.wt2lt_construction wtc
WHERE (w.tags @> wtl.tags
-- Nimm auch geplante oder im Bau befindliche Straßen mit
OR (w.tags @> wtc.tag1 AND w.tags @> wtc.tag2))
AND wtl.linktype_id=lt.id
AND wtc.linktype_id=lt.id
GROUP BY w.id;
CREATE INDEX streets_idx ON "{network}".streets USING btree(id);
ANALYZE "{network}".streets;
""".format(network=self.network)
        self.run_query(sql)

    def create_roads(self):
        """
        Create the view for the roads
        """
        self.logger.info(f'Create Roads')

        sql = """

-- Ways, die enthalten sind in Relationen vom type 'route', bei denen das tag 'motorcar'='yes' gesetzt ist
CREATE OR REPLACE VIEW "{network}".carferry AS
SELECT rm.member_id AS id
FROM
  osm.relation_members rm,
  osm.relations rel,
  (SELECT tags FROM classifications.access_types WHERE oeffne_pkw = TRUE) at
WHERE
rel.tags @> at.tags
AND rel.id = rm.relation_id
AND rm.member_type = 'W'
AND (rel.tags -> 'type') = 'route'
;


-- für Pkw erst mal geschlossene Straßen (bekommen closed=true). Dies sind entweder Fähren (Annahme: Personenfähren - s.category='D')
-- oder Straßen bei denen einer der access-tags die Straße für Pkws sperrt (bool_or(at.sperre_pkw) GROUP BY s.id)
CREATE OR REPLACE VIEW "{network}".closed_roads AS
SELECT DISTINCT s.id
FROM "{network}".streets s,
     osm.ways w,
     classifications.access_types at
     WHERE s.id = w.id
     AND ((at.sperre_pkw = TRUE AND w.tags @> at.tags)
     OR s.category = 'D')
     ;

-- für Pkw im- oder explizit geöffnete Straßen (bekommen closed=true):
-- oder Straßen bei denen einer der access-tags die Straße für Pkws explizit öffnet (bool_or(at.oeffne_pkw) GROUP BY s.id)
CREATE OR REPLACE VIEW "{network}".opened_roads AS
SELECT DISTINCT s.id
FROM "{network}".streets s,
     osm.ways w,
     classifications.access_types at
     WHERE (at.oeffne_pkw = TRUE)
     AND w.tags @> at.tags
     AND s.id = w.id
     ;


-- selektiere Straßen der Kategorie A-D (Autobahn bis Fähren ohne Fusswege),
-- Dies sind entweder Autofähren oder Autoreisezüge (carferry.carferry = true)
-- Oder Straßen, die für Pkw nicht geschlossen sind, oder bei denen bei closed k.A. da ist oder die explizit für Pkw geöffnet wurden
CREATE MATERIALIZED VIEW "{network}".roads AS
SELECT s.id, s.linktype_id, s.category
FROM "{network}".streets s
LEFT JOIN "{network}".carferry cf ON (s.id=cf.id)
LEFT JOIN "{network}".closed_roads clr ON (s.id=clr.id)
LEFT JOIN "{network}".opened_roads opr ON (s.id=opr.id)
WHERE
-- Straßen der Kategorie A-D
s.category <= 'D' AND
-- die nicht gesperrt sind
(clr.id IS NULL
-- Oder explizit geöffnet oder Fähren/Autoreisezüge
OR (opr.id IS NOT NULL OR cf.id IS NOT NULL));
""".format(network=self.network)
        self.run_query(sql)

    def create_junctions(self):
        """
        create junctions
        """
        self.logger.info(f'Create Junctions')
        sql = """
CREATE INDEX idx_roads_id ON "{network}".roads USING btree(id);
ANALYZE "{network}".roads;

-- die wayids der roads
CREATE MATERIALIZED VIEW "{network}".wayids AS
SELECT DISTINCT id AS wayid FROM "{network}".roads;
CREATE INDEX idx_wayids_wayid ON "{network}".wayids USING btree(wayid);


-- Selektiere die Knoten von Straßen (roadnodes),
-- die Teil von mehr als einer Road sind
-- (HAVING count(roadnodes.id) > 1) als junctions
CREATE TABLE "{network}".junctions
(id SERIAL,
nodeid bigint PRIMARY KEY,
geom geometry(POINT, {srid}),
pnt_wgs geometry(POINT, 4326));

INSERT INTO "{network}".junctions
SELECT
  row_number() OVER (ORDER BY a.nodeid)::integer AS id,
  a.nodeid,
  nodes.geom,
  st_transform(nodes.geom, 4326) AS pnt_wgs
FROM osm.nodes,
   ( SELECT roadnodes.nodeid, count(roadnodes.id) AS anzpunkte
     FROM ( SELECT wn.way_id AS id, wn.node_id AS nodeid
            FROM osm.way_nodes wn, "{network}".roads r
            WHERE wn.way_id = r.id
          ) roadnodes
     GROUP BY roadnodes.nodeid
     HAVING count(roadnodes.id) > 1
   ) a
WHERE a.nodeid = nodes.id;

-- Add dangling Endnodes as junctions

INSERT INTO "{network}".junctions (nodeid, geom, pnt_wgs)

SELECT
n.id AS nodeid, n.geom, st_transform(n.geom, 4326) As pnt_wgs
FROM osm.ways w
LEFT JOIN "{network}".junctions j ON j.nodeid = w.nodes[1],
 "{network}".roads r,
 osm.nodes n
WHERE j.id IS NULL
AND w.id = r.id
AND w.nodes[1] = n.id
UNION
SELECT
n.id AS nodeid, n.geom, st_transform(n.geom, 4326) As pnt_wgs
FROM osm.ways w
LEFT JOIN "{network}".junctions j ON j.nodeid = w.nodes[array_upper(w.nodes, 1)],
 "{network}".roads r,
 osm.nodes n
WHERE j.id IS NULL
AND w.id = r.id
AND w.nodes[array_upper(w.nodes, 1)] = n.id
;

CREATE INDEX idx_junctions_geom ON "{network}".junctions USING gist(geom);
ANALYZE "{network}".junctions;


-- Berechne Z-Koordinaten
CREATE OR REPLACE VIEW "{network}".junctions_z_interpolated AS
WITH dx AS (SELECT generate_series(-1, 1) AS dx),
dy AS (SELECT generate_series(-1, 1) AS dy),
jr AS (
  SELECT
  j.nodeid,
  j.pnt_wgs,
  a.rast,
  ST_WorldToRasterCoordX(a.rast, j.pnt_wgs) AS colx,
  ST_WorldToRasterCoordY(a.rast, j.pnt_wgs) AS rowy,
  st_height(a.rast) AS height,
  st_width(a.rast) AS width
FROM "{network}".junctions j,
landuse.aster a
WHERE st_intersects(j.pnt_wgs, a.rast))

SELECT
c.nodeid,
sum(c.val) / sum(c.weight) As z
FROM (
SELECT
b.nodeid,
exp({beta} * b.distance) AS weight,
b.val * exp({beta} * b.distance) AS val
FROM
(
SELECT
jr.nodeid,
st_distance(
ST_PixelAsCentroid(
  jr.rast, jr.colx + dx.dx, jr.rowy + dy.dy)::geography,
  jr.pnt_wgs::geography) AS distance,
st_value(
    jr.rast, jr.colx + dx.dx, jr.rowy + dy.dy) AS val
FROM jr, dx, dy
WHERE 0 < jr.colx + dx.dx
AND jr.colx + dx.dx <= jr.width
AND 0 < jr.rowy + dy.dy
AND jr.rowy + dy.dy <= jr.height) b
WHERE b.distance < {max_dist}
) c
WHERE c.val IS NOT NULL
GROUP BY c.nodeid;

-- Berechne Z-Koordinaten der Junctions
CREATE MATERIALIZED VIEW "{network}".junctions_z AS
SELECT
a.nodeid,
COALESCE(e.z, a.z) AS z
FROM "{network}".junctions_z_interpolated a
-- Falls Nodes eine Höhenangabe haben, nimm diese (nur Ziffern und .)
LEFT JOIN
(SELECT n.id,
substring(tags -> 'ele' FROM '[-+]?\d*\.\d+|\d+')::double precision AS z
FROM osm.nodes n, "{network}".junctions AS p
WHERE p.nodeid = n.id
AND n.tags ? 'ele'
AND n.tags <> ''::hstore) e
ON a.nodeid = e.id
;

CREATE INDEX pk_junctions_z ON "{network}".junctions_z USING btree(nodeid);

"""
        self.run_query(sql.format(beta=-0.1,  # Gewichtungsfaktor weight = exp(beta * meter)
                                  max_dist=30,  # Maximale Distanz zu benachbarten Centroiden
                                  network=self.network,
                                  srid=self.srid,
                                  ))

        #

        sql = """

-- markiere in in der inneren Select-Abfrage die junctions in der nodelist
-- (isjunction) und zähle die Anzahl der Knoten (lastindex)
-- in der äußeren Abfrage setze dann nur die Junctions auch wirklich auf 'isjunction'=1,
-- die nicht am Anfang (wm.idx>0) oder am Ende (wm.idx < (wm.lastindex-1)) des Weges sind,
-- da der Weg nur in der Mitte aufgespalten werden soll
CREATE OR REPLACE VIEW "{network}".way_marked_junctions AS
SELECT
  wm.id,
  wm.idx,
  wm.nodeid,
  (wm.idx < (wm.lastindex - 1) AND wm.idx > 0 AND wm.isjunction IS NOT NULL)::integer AS isjunction
FROM ( SELECT
          wn.way_id AS id,
          wn.sequence_id AS idx,
          wn.node_id AS nodeid,
          j.nodeid AS isjunction,
          count(*) OVER (PARTITION BY wn.way_id) AS lastindex
       FROM
          osm.way_nodes wn LEFT JOIN "{network}".junctions j ON wn.node_id = j.nodeid,
          "{network}".roads r
       WHERE wn.way_id = r.id
     ) wm;

-- erstelle einzelne Segmente aus den ways, in dem an nodes, die junctions sind, der Weg aufgespalten wird
-- zähle dazu die Segmente eines Wegs (PARTITION BY id) durch, indem die Zahl der Junctions (sum(isjunction) bis zum aktuellen node (PRECEDING) aufsummiert wird
-- dabei wird der Weg nach dem idx aufsteigend sortiert (ORDER BY idx)
CREATE OR REPLACE VIEW "{network}".ways2links AS
SELECT
  id, idx, nodeid, isjunction,
  sum(isjunction) OVER (PARTITION BY id ORDER BY idx RANGE UNBOUNDED PRECEDING) AS segment
FROM "{network}".way_marked_junctions
GROUP BY id, idx, nodeid, isjunction
ORDER BY id, idx;


-- erstelle Tabelle link_points
DROP TABLE IF EXISTS "{network}".link_points;
CREATE TABLE "{network}".link_points
(
  wayid bigint NOT NULL,
  segment integer NOT NULL,
  idx integer NOT NULL,
  nodeid bigint
)
WITH (
  OIDS=FALSE
);
ALTER TABLE "{network}".link_points ADD PRIMARY KEY (wayid, segment, idx);
CREATE INDEX link_points_nodeid ON "{network}".link_points (nodeid);


-- erstelle Spalten mit dem ersten und letzten Knoten
CREATE MATERIALIZED VIEW "{network}".link_from_to_node AS
SELECT DISTINCT ON (wayid, segment)
  wayid,
  segment,
  first_value(nodeid) OVER (PARTITION BY wayid, segment ORDER BY idx) AS fromnode,
  last_value(nodeid) OVER (PARTITION BY wayid, segment ORDER BY idx) AS tonode
FROM "{network}".link_points
ORDER BY wayid, segment, idx DESC;

        """.format(network=self.network)
        self.run_query(sql)

    def drop_temp_wayids(self):
        sql = '''
DROP TABLE IF EXISTS "{network}".temp_wayids;
        '''.format(network=self.network)
        self.run_query(sql)

    def fill_link_points_and_create_links(self):
        """
        fill link points and create links
        """
        sql = """
-- fülle Tabelle link_points mit "Knoten" und lösche dazu vorher den Index
ALTER TABLE "{network}".link_points DROP CONSTRAINT link_points_pkey;
DROP INDEX "{network}".link_points_nodeid;

TRUNCATE "{network}".link_points;
SELECT "{network}".createlink_points(500000000,0);
ALTER TABLE "{network}".link_points ADD PRIMARY KEY (wayid, segment, idx);
CREATE INDEX link_points_nodeid ON "{network}".link_points USING btree(nodeid);
""".format(network=self.network)
        self.run_query(sql)
        sql = """
-- füge noch die endpoints ein
SELECT "{network}".insertendpoints(500000000,0);

-- erstelle Tabelle mit Start und Endpunkten der links
REFRESH MATERIALIZED VIEW "{network}".link_from_to_node;

        """.format(network=self.network)
        self.run_query(sql)

    def create_links(self):
        """
        iterate over the chunks and create links
        """
        self.logger.info(f'Create Links')
        sql = """
DROP TABLE IF EXISTS "{network}".links;
CREATE TABLE "{network}".links
(
  id SERIAL,
  fromnode bigint,
  tonode bigint,
  wayid bigint NOT NULL,
  segment integer NOT NULL,
  geom geometry(LINESTRING, {srid}),
  linkname text,
  linkref text,
  linktype integer,
  maxspeed integer,
  io boolean NOT NULL DEFAULT false,
  t_kfz double precision,
  oneway boolean NOT NULL DEFAULT false,
  planned boolean NOT NULL DEFAULT false,
  construction boolean NOT NULL DEFAULT false,
  lanes smallint DEFAULT 2,
  bridge_tunnel "char" NOT NULL DEFAULT ''::"char",
  slope double precision DEFAULT 0
)
WITH (
  OIDS=FALSE
);
        """
        self.run_query(sql.format(srid=self.srid, network=self.network))

    def fill_links(self):
        """
        fill the links
        """
        self.logger.debug(f'Fill Links')
        network = self.network
        limit = self.limit or 'NULL'
        sql = f"""
TRUNCATE "{network}".links;
SELECT "{network}".create_links({limit}, 0);

-- lösche links ohne Geometrie
DELETE FROM "{network}".links WHERE st_NumPoints(geom) = 0;
        """
        self.run_query(sql)

    def create_functions(self):
        """
        Create functions to create link_points and links
        """
        self.logger.debug(
            f'Create Functions to create link_points and links')
        cur = self.conn.cursor()
        sql = '''

CREATE OR REPLACE FUNCTION "{network}".createlink_points(lim int, offs int)
  RETURNS integer AS
$BODY$
DECLARE
   fl RECORD;
   fromRow integer DEFAULT -1;
   toRow integer;
   maxRow integer;
BEGIN
SELECT max(wayid) INTO maxRow FROM "{network}".wayids;
FOR fl IN
(SELECT row_number() OVER (ORDER BY wayid) As rowid, wayid
FROM "{network}".wayids LIMIT $1 OFFSET $2)
LOOP
  IF mod(fl.rowid, {chunksize}) = 0 OR fl.wayid = maxRow THEN
    toRow := fl.wayid;
    RAISE NOTICE 'Erstelle link_points für way % to %', fromRow, toRow;
    INSERT INTO "{network}".link_points (wayid,segment,idx,nodeid)
    SELECT id,segment,idx,nodeid
    FROM
    (
    SELECT
      way_marked_junctions.id,
      way_marked_junctions.idx,
      way_marked_junctions.nodeid,
      way_marked_junctions.isjunction,
      sum(way_marked_junctions.isjunction) OVER (PARTITION BY way_marked_junctions.id
                                                 ORDER BY way_marked_junctions.idx
                                                 RANGE UNBOUNDED PRECEDING) AS segment
    FROM
        (
        SELECT
          wm.id,
          wm.idx,
          wm.nodeid,
          (wm.idx < (wm.lastindex - 1) AND wm.idx > 0 AND wm.isjunction IS NOT NULL)::integer AS isjunction
        FROM
            (SELECT
               wn.way_id AS id,
               wn.sequence_id AS idx,
               wn.node_id AS nodeid,
               j.nodeid AS isjunction,
               count(*) OVER (PARTITION BY wn.way_id) AS lastindex
             FROM osm.way_nodes wn
             LEFT JOIN "{network}".junctions j ON (wn.node_id = j.nodeid),
             "{network}".roads r
             WHERE wn.way_id = r.id AND r.id > fromRow AND r.id <= toRow
             ) wm
       ) way_marked_junctions
    GROUP BY way_marked_junctions.id,
             way_marked_junctions.idx,
             way_marked_junctions.nodeid,
             way_marked_junctions.isjunction
    ORDER BY way_marked_junctions.id,
             way_marked_junctions.idx
    ) wl
    ;
    fromRow := toRow;
  END IF;
END LOOP;
RETURN fl.rowid;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
  COMMIT;
  '''.format(chunksize=self.chunksize, network=self.network)
        cur.execute(sql)

        sql = '''
-- An den Junctions füge den Knoten noch mal als letzten Punkt des vorhergehende Segment (segment-1) ein
-- Startpunkt des nächsten Segments ist auch letzter Punkt des vorhergenenden Segments eines ways
-- erstmal in temporäre Tabelle schreiben

CREATE OR REPLACE FUNCTION "{network}".insertendpoints(lim int, offs int)
  RETURNS integer AS
$BODY$
DECLARE
   fl RECORD;
   fromRow integer DEFAULT -1;
   toRow integer;
   maxRow integer;
BEGIN
  SELECT max(wayid) INTO maxRow
  FROM "{network}".wayids LIMIT $1 OFFSET $2;
  FOR fl IN
  (SELECT row_number() OVER (ORDER BY wayid) As rowid, wayid
   FROM "{network}".wayids LIMIT $1 OFFSET $2)
LOOP
IF mod(fl.rowid,10000) = 0 OR fl.wayid = maxRow THEN
    toRow := fl.wayid;
    RAISE NOTICE 'Erstelle Endpunkte für way % bis %', fromRow, toRow;

INSERT INTO "{network}".link_points
SELECT a.wayid, a.segment, a.idx, a.nodeid
FROM
(SELECT
  lp.wayid,
  lp.segment-1 AS segment,
  idx,
  lp.nodeid,
  max(idx) OVER(PARTITION BY wayid ) AS lastindex,
  (j.nodeid is not null) As junct
FROM
  "{network}".link_points lp LEFT JOIN "{network}".junctions j ON (lp.nodeid=j.nodeid)
  WHERE wayid > fromRow AND wayid <= toRow
) a
WHERE
  junct and
  idx > 0 AND idx < lastindex
;
fromRow := toRow;
END IF;
END LOOP;
RETURN fl.rowid;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
  COMMIT;
  '''.format(network=self.network)
        cur.execute(sql)

        sql = """
-- DROP FUNCTION create_links();

-- bilde links aus den link_points. Verknüpfe dazu in der inneren Selection die Attribute des links (wayid, from&to-node des segments, Geometrie der Knoten)
-- und gruppiere dann die Knoten


CREATE OR REPLACE FUNCTION "{network}".create_links(lim int, offs int)
  RETURNS integer AS
$BODY$
DECLARE
   fl RECORD;
BEGIN
  FOR fl IN
  (SELECT row_number() OVER (ORDER BY id desc)* {chunksize} AS todo,
          COALESCE(lag(id,1) OVER(ORDER BY id),-1) AS rowidfrom,
          id AS rowidto FROM "{network}".wayid_chunk LIMIT $1 OFFSET $2)
  LOOP
	INSERT INTO "{network}".links (fromnode, tonode, wayid, segment, geom,
                               linkname, linkref)
    SELECT l.fromnode, l.tonode, l.wayid, l.segment, l.geom,
    w.tags -> 'name' AS linkname, w.tags -> 'ref' AS linkref
    FROM
    (
	SELECT fromnode, tonode, wayid, segment, St_MakeLine(geom ORDER BY lp.idx ASC) geom
	FROM
	 (SELECT ft.fromnode,
		 ft.tonode,
		 lp.wayid AS wayid,
		 lp.segment as segment,
		 n.geom AS geom,
		 lp.idx idx
	  FROM
	    "{network}".link_points lp,
	    "{network}".link_from_to_node AS ft,
	    osm.nodes n
	  WHERE
	    lp.wayid = ft.wayid and lp.segment = ft.segment
	    and lp.nodeid = n.id
	    AND lp.wayid>fl.rowidfrom and lp.wayid <= fl.rowidto
	    AND ft.wayid>fl.rowidfrom and ft.wayid <= fl.rowidto
	)lp
	GROUP BY fromnode, tonode, wayid, segment) AS l,
    osm.ways w
    WHERE w.id = l.wayid;
    RAISE NOTICE 'Noch % links: Erstelle links von % bis %', fl.todo, fl.rowidfrom, fl.rowidto;
  END LOOP;
RETURN fl.rowidto;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
COMMIT;
        """.format(chunksize=self.chunksize, network=self.network)
        cur.execute(sql)

    def update_linktypes(self):
        """
        Update the Linktypes
        """
        self.logger.debug(
            'Update the Linktypes by spatial intersection with urban area')
        corine = self.corine[0]
        sql = """
UPDATE "{network}".links l
SET linktype = s.linktype_id
FROM "{network}".streets s
WHERE l.wayid = s.id;

CREATE INDEX idx_links_geom ON "{network}".links USING gist (geom);
ANALYZE "{network}".links;

WITH urban AS
(SELECT c.geom
FROM landuse.{corine} c
WHERE c.code IN (SELECT code FROM classifications.corine_urban))

UPDATE "{network}".links l
SET io = TRUE
FROM urban
WHERE st_intersects(urban.geom, l.geom);


CREATE INDEX idx_links_linktype ON "{network}".links USING btree (linktype, io);

UPDATE "{network}".links l
SET
  planned = TRUE,
  linktype = lt.linktype_id
FROM osm.ways w, classifications.wt2lt_construction lt
WHERE l.wayid = w.id
AND w.tags ? 'proposed'
AND w.tags @> lt.tag1
AND w.tags @> lt.tag2;

UPDATE "{network}".links l
SET
  construction = TRUE,
  linktype = lt.linktype_id
FROM osm.ways w, classifications.wt2lt_construction lt
WHERE l.wayid = w.id
AND w.tags ? 'construction'
AND w.tags @> lt.tag1
AND w.tags @> lt.tag2;
""".format(network=self.network,
           corine=corine)
        self.run_query(sql)

    def update_speed(self):
        """
        """
        self.logger.debug('Set Maxspeed and Average Speed')

        sql = """
-- erzeuge speed_zulaessig
ALTER TABLE "{network}".links
  ADD COLUMN speed_zulaessig INTEGER;
ALTER TABLE "{network}".links
  ALTER COLUMN speed_zulaessig SET DEFAULT 50;

-- setze maxspeed aus tags
UPDATE "{network}".links l
SET maxspeed = substring(w.tags -> 'maxspeed' FROM '[-+]?\d*\.\d+|\d+')::float,
    speed_zulaessig = substring(w.tags -> 'maxspeed' FROM '[-+]?\d*\.\d+|\d+')::float::integer
FROM osm.ways w
WHERE l.wayid=w.id and w.tags ? 'maxspeed';

-- setze maxspeed auf 120 für BAB-Abschnitte mit Wechselanzeigen
UPDATE "{network}".links l
SET maxspeed = 120,
    speed_zulaessig = 120
FROM osm.ways w
WHERE l.wayid=w.id and w.tags -> 'maxspeed' = 'signals';

-- setze maxspeed auf 150 für BAB-Abschnitte ohne Geschwindigkeitsbegrenzung
UPDATE "{network}".links l
SET maxspeed = 150,
    speed_zulaessig = 150
FROM osm.ways w
WHERE l.wayid=w.id and w.tags -> 'maxspeed' = 'none';

UPDATE "{network}".links l
SET maxspeed = ld.v_kfz,
    speed_zulaessig = ld.v_kfz_zulaessig
FROM classifications.link_defaults ld, classifications.linktypes lt
WHERE
ld.linktype_number = lt.id AND
ld.linktype_number = l.linktype AND ld.innerorts = l.io
AND l.maxspeed IS NULL;

UPDATE "{network}".links l
SET maxspeed = ld.v_kfz
FROM classifications.link_defaults ld, classifications.linktypes lt
WHERE
ld.linktype_number = lt.id AND
ld.linktype_number = l.linktype AND ld.innerorts = l.io
AND ld.v_kfz < l.maxspeed and lt.road_category >'A';
        """.format(network=self.network)
        self.run_query(sql)

    def update_oneway(self):
        """
        Update oneway streets
        """
        self.logger.debug('Update Oneway Streets')
        sql = """
UPDATE "{network}".links l
SET oneway = TRUE
FROM osm.ways w
WHERE
(  w.tags -> 'oneway' = 'motor_vehicle' OR
  w.tags -> 'oneway' = 'true' OR
  w.tags -> 'oneway' = 'yes' OR
  w.tags -> 'junction' = 'roundabout' OR
  w.tags -> 'oneway' = '1')
AND w.id = l.wayid
;

UPDATE "{network}".links l
SET oneway = FALSE
FROM osm.ways w
WHERE
  w.tags -> 'oneway' = 'no'
AND w.id = l.wayid
;

-- drehe links mit oneway = -1 und setze oneway auf True
UPDATE "{network}".links l
SET oneway = TRUE,
    fromnode = tonode,
    tonode = fromnode,
    geom = st_reverse(geom)
FROM osm.ways w
WHERE
  w.tags -> 'oneway' = '-1'
AND w.id = l.wayid
;
        """.format(network=self.network)
        self.run_query(sql)

    def update_time(self):
        """
        Update Travel Times
        """
        self.logger.debug('Update travel times for links')

        sql = """
UPDATE "{network}".links l
SET
  t_kfz = CASE
    WHEN l.maxspeed = 0
    THEN 9999999
    ELSE st_length(l.geom) / l.maxspeed * 3.6 / 60
    END;

-- Ferries
UPDATE "{network}".links l
SET
  t_kfz = g.duration
FROM
(
  SELECT
    f.wayid,
    f.segment,
    CASE
      WHEN rn=1
      THEN f.duration
      ELSE 0.01
    END AS duration
FROM
(SELECT
    l.wayid, l.segment,
    substring(w.tags -> 'duration', '[-+]?\d*\.\d+|\d+')::double precision AS duration,
    row_number() OVER (PARTITION BY l.wayid ORDER BY st_length(l.geom) DESC) AS rn
FROM
 "{network}".links l,
 classifications.linktypes lt,
 osm.ways w
WHERE lt.road_category = 'D'
AND w.tags ? 'duration'
AND l.wayid = w.id AND l.linktype = lt.id) f
) g
WHERE l.wayid = g.wayid AND l.segment = g.segment;
        """.format(network=self.network)
        self.run_query(sql)

    def update_lanes(self):
        """
        """
        self.logger.debug('Update number of lanes')
        sql = """
UPDATE "{network}".links l
SET lanes = 1
WHERE l.oneway;

UPDATE "{network}".links l
SET lanes = substring(w.tags->'lanes' FROM '[0-9]+')::integer
FROM osm.ways w
WHERE l.wayid=w.id AND w.tags ? 'lanes';

UPDATE "{network}".links l
SET bridge_tunnel = 'b'
FROM osm.ways w
WHERE l.wayid=w.id AND w.tags ? 'bridge' AND w.tags->'bridge' <> 'no';
;

UPDATE "{network}".links l
SET bridge_tunnel = 't'
FROM osm.ways w
WHERE l.wayid=w.id AND w.tags ? 'tunnel' AND w.tags->'tunnel' <> 'no';
;

UPDATE "{network}".links l
SET linkname = linkref
WHERE linkname IS NULL AND linkref IS NOT NULL;
        """.format(network=self.network)
        self.run_query(sql)

    def create_slope(self):
        """
        """
        self.logger.debug('calculate slope of links')
        sql = """
UPDATE "{network}".links l
SET slope = CASE
            WHEN st_length(l.geom)=0
            THEN 0
            ELSE (tn.z-fn.z) / st_length(l.geom)
            END
FROM
"{network}".junctions_z fn,
"{network}".junctions_z tn
WHERE fn.nodeid = l.fromnode
AND tn.nodeid = l.tonode
AND bridge_tunnel = '';
        """.format(network=self.network)
        self.run_query(sql)

    def create_chunks(self):
        self.logger.debug('Create chunks')
        sql = """
-- erzeuge Tabelle mit jeder 1.000sten wayid, um die links in Häppchen (chunks) zu erzeugen

DROP TABLE IF EXISTS "{network}".wayid_chunk;
SELECT DISTINCT w.wayid AS id INTO "{network}".wayid_chunk
FROM
  (SELECT wayid, row_number() OVER(ORDER BY ws.wayid,ws.segment) rn
   FROM
     (SELECT DISTINCT wayid,segment FROM "{network}".link_points) ws
   ) w
WHERE mod(w.rn, {chunksize}) = 0;
INSERT INTO "{network}".wayid_chunk SELECT max(wayid) AS id FROM "{network}".link_points;
ALTER TABLE "{network}".wayid_chunk ADD PRIMARY KEY(id);
COMMIT;

        """.format(chunksize=self.chunksize, network=self.network)
        self.run_query(sql)

    def create_barriers(self):
        """
        Create Barriers
        """
        self.logger.debug('Create Barriers')
        sql = """
CREATE OR REPLACE VIEW "{network}".barriers_car AS
 SELECT b.id,
    b.geom::geometry(POINT, {srid}) AS geom,
    COALESCE(b.closed, false) AS explicitly_closed,
    b.tags -> 'barrier'::text AS barrier_type,
    b.tags -> 'note'::text AS note,
    b.tags
   FROM ( SELECT n.id,
            n.tags,
            bool_or(a.sperre_pkw) AS closed,
            bool_or(a.oeffne_pkw) AS opened,
            n.geom
           FROM "{network}".link_points lp,
            osm.nodes n
             LEFT JOIN classifications.access_types a ON n.tags @> a.tags
          WHERE n.id = lp.nodeid
          AND n.tags ? 'barrier'::text
          AND n.tags <> ''::hstore
          GROUP BY n.id
         HAVING bool_or(a.sperre_pkw) OR (bool_or(a.oeffne_pkw) IS NULL)
         )b
;

CREATE OR REPLACE VIEW "{network}".line_barriers_car AS
 SELECT b.id,
    b.geom::geometry(LINESTRING, {srid}) AS geom,
    COALESCE(b.closed, false) AS explicitly_closed,
    b.tags -> 'barrier'::text AS barrier_type,
    b.tags -> 'note'::text AS note,
    b.tags
   FROM ( SELECT w.id,
            w.tags,
            bool_or(a.sperre_pkw) AS closed,
            bool_or(a.oeffne_pkw) AS opened,
            w.linestring AS geom
           FROM "{network}".link_points lp,
            osm.way_nodes wn,
            osm.ways w
             LEFT JOIN classifications.access_types a ON w.tags @> a.tags
          WHERE w.id = wn.way_id AND
          wn.node_id = lp.nodeid AND w.tags ? 'barrier'::text
          GROUP BY w.id
         HAVING bool_or(a.sperre_pkw) OR (bool_or(a.oeffne_pkw) IS NULL)
        ) b
;
        """.format(network=self.network, srid=self.srid)
        self.run_query(sql)

    def create_index(self):
        """
        """
        self.logger.info('Create Indices')
        sql = """
ALTER TABLE "{network}".links ADD PRIMARY KEY (wayid, segment);
CREATE INDEX idx_links_oneway ON "{network}".links USING btree (oneway) WHERE oneway = TRUE;
CREATE INDEX idx_links_planned ON "{network}".links USING btree (planned) WHERE planned = TRUE;
CREATE INDEX idx_links_construction ON "{network}".links USING btree (construction) WHERE construction = TRUE;
CREATE INDEX idx_links_name ON "{network}".links USING btree (linkname);
CREATE INDEX idx_links_ref ON "{network}".links USING btree (linkref) WHERE linkref <> '';
CREATE INDEX idx_links_bridge ON "{network}".links USING btree (bridge_tunnel) WHERE bridge_tunnel <> '';
CREATE INDEX idx_links_fromnode_tonode ON "{network}".links USING btree (fromnode, tonode);
CLUSTER "{network}".links USING idx_links_geom;
        """.format(network=self.network)
        self.run_query(sql)

    def create_view_accessible_links(self):
        """
        Create the views for the accessible links
        """
        self.logger.info('Create View with accessible links')
        sql = """
CREATE OR REPLACE VIEW "{network}".links_reached_without_planned AS
SELECT l.*, lt.road_category
FROM
  "{network}".links l,
  classifications.linktypes lt,
  "{network}".edges_reached e
WHERE l.wayid=e.wayid AND l.segment=e.segment
AND l.linktype = lt.id;

CREATE OR REPLACE VIEW "{network}".links_reached_only_by_planned AS
SELECT l.*, lt.road_category
FROM
  "{network}".links l,
  classifications.linktypes lt,
  "{network}".edges_reached_with_planned ep
  LEFT JOIN "{network}".edges_reached e ON ep.id = e.id
WHERE l.wayid=ep.wayid AND l.segment=ep.segment
AND e.id IS NULL
AND l.linktype = lt.id;

CREATE OR REPLACE VIEW "{network}".unaccessible_links AS
SELECT lt.road_category, l.*
FROM
  classifications.linktypes lt,
  "{network}".links l LEFT JOIN
  "{network}".edges_reached_with_planned e
  ON l.wayid=e.wayid AND l.segment=e.segment
WHERE e.id IS NULL
AND l.linktype=lt.id;
""".format(network=self.network)
        self.run_query(sql)

    def create_views_roadtypes(self):
        """
        """
        self.logger.info('Create Views with different roadtypes')
        sql = """
-- erstelle View für die einzelnen Streckentypen
CREATE OR REPLACE VIEW "{network}".autobahn AS
SELECT l.*
FROM
  "{network}".links_reached_without_planned l
  WHERE road_category='A';

CREATE OR REPLACE VIEW "{network}".hauptstr AS
SELECT l.*
FROM
  "{network}".links_reached_without_planned l
  WHERE road_category='B';

CREATE OR REPLACE VIEW "{network}".nebennetz AS
SELECT l.*
FROM
  "{network}".links_reached_without_planned l
  WHERE road_category='C';

CREATE OR REPLACE VIEW "{network}".faehren AS
SELECT l.*
FROM
  "{network}".links_reached_without_planned l
  WHERE road_category='D';

CREATE OR REPLACE VIEW "{network}".autobahn_accessible_only_by_planned AS
SELECT l.*
FROM
  "{network}".links_reached_only_by_planned l
  WHERE road_category='A';

CREATE OR REPLACE VIEW "{network}".hauptstr_accessible_by_planned AS
SELECT l.*
FROM
  "{network}".links_reached_only_by_planned l
  WHERE road_category='B';

CREATE OR REPLACE VIEW "{network}".nebennetz_accessible_by_planned AS
SELECT l.*
FROM
  "{network}".links_reached_only_by_planned l
  WHERE road_category='C';

CREATE OR REPLACE VIEW "{network}".faehren_accessible_by_planned AS
SELECT l.*
FROM
  "{network}".links_reached_only_by_planned l
  WHERE road_category='D';

        """.format(network=self.network)
        self.run_query(sql)

    def create_pgrouting_network(self):
        """
        Create a pg_routing network from the links-table
        """
        self.logger.info(
            'Create routable network to check network connectivity')
        sql = """
CREATE TABLE "{network}".edge_table (
id INTEGER PRIMARY KEY,
fromnode bigint,
tonode bigint,
linkid bigint,
geom geometry(LineString,{srid}),
"source" integer,
"target" integer,
cost float,
reverse_cost float,
wayid bigint,
segment integer);

CREATE INDEX edge_table_geom_idx ON "{network}".edge_table USING gist(geom);
CREATE INDEX edge_table_ft_idx ON "{network}".edge_table USING btree(fromnode, tonode);
CREATE INDEX edge_table_link_idx ON "{network}".edge_table USING btree(linkid);
CREATE INDEX edge_table_source_idx ON "{network}".edge_table USING btree("source");
CREATE INDEX edge_table_target_idx ON "{network}".edge_table USING btree("target");
""".format(srid=self.srid, network=self.network)
        self.run_query(sql)

    def update_edge_table_with_construction(self):
        """
        close edges with construction or planned
        """
        self.logger.info('Update edge table with constructions')
        sql = """
UPDATE "{network}".edge_table e
SET cost = -1, reverse_cost = -1
FROM "{network}".links l
WHERE
e.fromnode=l.fromnode AND e.tonode=l.tonode AND
(l.planned OR l.construction);
        """.format(network=self.network)
        self.run_query(sql)

    def update_egde_table(self):
        """
        Updates the edge_table
        """
        self.logger.info('Update Edge Table')
        sql = """

TRUNCATE "{network}".edge_table;
INSERT INTO "{network}".edge_table (id, fromnode, tonode, linkid, geom,
cost, reverse_cost, wayid, segment)
SELECT
  row_number() OVER (ORDER BY fromnode, tonode)::integer AS id,
  fromnode,
  tonode,
  l.id,
  l.geom,
  l.t_kfz AS cost,
  CASE WHEN l.oneway THEN -1 ELSE l.t_kfz END AS reverse_cost,
  l.wayid,
  l.segment
FROM "{network}".links l;
        """.format(network=self.network)
        self.run_query(sql)

    def create_topology(self):
        """
        create and analyze the topology
        """
        self.logger.info('Create Topology')

        chunksize = 50000
        cursor = self.conn.cursor()
        sql = 'SELECT max(e.id) FROM "{network}".edge_table e;'
        cursor.execute(sql.format(network=self.network))
        n_edges = cursor.fetchone()[0]

        for fromrow in range(0, n_edges, chunksize):
            torow = fromrow + chunksize
            sql = """
SELECT pgr_createTopology(E'{network}.edge_table',
    {tolerance},
    'geom',
    'id',
    'source',
    'target',
    rows_where := 'id >= {fromrow} AND id < {torow}',
    clean := false::boolean);
COMMIT;
""".format(tolerance=0.000001, network=self.network,
                fromrow=fromrow, torow=torow)
            self.logger.info(
                'create topology for edges {} to {}'.format(fromrow, torow))
            self.run_query(sql)

        sql = """
SELECT pgr_analyzeGraph(E'{network}.edge_table',{tolerance},
'geom','id','source','target');
        """.format(tolerance=0.000001, network=self.network)
        self.logger.info('Analyze Graph')
        self.run_query(sql)

    def create_edge_reached(self):
        """
        """
        self.logger.info(
            'Create Materialized Views with edges reached from a start vertex')

        # init the driving distance queries
        self.update_pgr_driving_distance()
        # create queries
        sql = """
CREATE MATERIALIZED VIEW "{network}".reached_from_mat AS
SELECT * FROM "{network}".reached_from
WITH NO DATA;

CREATE MATERIALIZED VIEW "{network}".reached_to_mat AS
SELECT * FROM "{network}".reached_to
WITH NO DATA;

CREATE INDEX idx_node_reached_from ON "{network}".reached_from_mat
USING btree(node);
CREATE INDEX idx_node_reached_to ON "{network}".reached_to_mat
USING btree(node);

CREATE MATERIALIZED VIEW "{network}".edges_reached AS
-- edges reached in both directions
SELECT e.id, e.fromnode, e.tonode, e.wayid, e.segment
FROM
    "{network}".edge_table e,
    "{network}".reached_from_mat h,
    "{network}".reached_to_mat r
WHERE
    e."source" = h.node AND
    e."target" = r.node
WITH NO DATA;

CREATE INDEX idx_id_edges_reached ON "{network}".edges_reached
USING btree(id);
CREATE INDEX idx_nodes_edges_reached ON "{network}".edges_reached
USING btree(fromnode, tonode);

DROP TABLE IF EXISTS "{network}".edges_reached_with_planned CASCADE;
CREATE TABLE "{network}".edges_reached_with_planned
(id integer primary key,
fromnode bigint,
tonode bigint,
wayid bigint,
segment integer);
CREATE INDEX idx_nodes_edges_reached_with_planned
ON "{network}".edges_reached_with_planned
USING btree(fromnode, tonode);

CREATE OR REPLACE VIEW "{network}".vertexes_reached AS
-- vertexes reached in both directions
SELECT h.node
FROM
  "{network}".reached_from_mat h,
  "{network}".reached_to_mat r
WHERE h.node = r.node;
        """.format(network=self.network)
        self.run_query(sql)

    def try_startvertices(self, n=20, k=4):
        """
        search vertices in the surrounding of the centroid of the vertices
        or the biggest cluster of vertices

        Parameters
        ----------
        n : int
            the total number of vertices to test
        k : int
            the number of clusters to test
        """
        self.logger.info('search from different clusters of vertices')

        sql = """
WITH cluster AS
(SELECT c.s, c.geom
FROM
(SELECT
  k.s,
  k.kmeans,
  row_number() OVER(PARTITION BY k.s ORDER BY count(*) DESC) rn,
  ST_Centroid(ST_Collect(k.geom)) AS geom
FROM (
  SELECT
    cl.s,
    kmeans(ARRAY[ST_X(j.the_geom), ST_Y(j.the_geom)], cl.s) OVER (PARTITION BY cl.s),
    j.the_geom AS geom
  FROM "{network}".edge_table_vertices_pgr j,
  (SELECT generate_series(1, {k}) s) cl
) AS k
GROUP BY k.s, k.kmeans) c
WHERE c.rn = 1)

SELECT e.id, e.geom, e.s, e.rn
FROM (
SELECT v.id , v.the_geom AS geom,
cluster.s,
row_number() OVER (PARTITION BY cluster.s ORDER BY v.the_geom <-> cluster.geom) AS rn
FROM "{network}".edge_table_vertices_pgr v, cluster) e
ORDER BY e.rn, e.s
LIMIT {n};
        """.format(n=n, k=k, network=self.network)
        cursor = self.conn.cursor()
        cursor.execute(sql)
        vertices = cursor.fetchall()

        sql = """
SELECT count(*) FROM "{network}".links;
        """.format(network=self.network)
        cursor.execute(sql)
        row = cursor.fetchone()
        self.n_links = row.count
        self.logger.info('{n} links in total'.format(n=self.n_links))

        sql_count_result = """
REFRESH MATERIALIZED VIEW "{network}".reached_from_mat;
REFRESH MATERIALIZED VIEW "{network}".reached_to_mat;
REFRESH MATERIALIZED VIEW "{network}".edges_reached;
SELECT count(*) FROM "{network}".edges_reached;
        """.format(network=self.network)

        i = 0
        msg = '{i} try to search accessible edges from vertex {v}'

        for vertex in vertices:
            i += 1
            self.update_pgr_driving_distance(startvertex=vertex.id,
                                             maxcosts=10000000)

            self.logger.info(msg.format(i=i, v=vertex.id))
            self.logger.debug(sql_count_result)
            cursor.execute(sql_count_result)
            row = cursor.fetchone()
            links_reached = float(row.count)
            msg2 = '{f} out of {n} edges reached'
            self.logger.info(msg2.format(
                f=int(links_reached), n=self.n_links))
            if links_reached > self.n_links * self.links_to_find:
                sql = 'ANALYZE "{network}".edges_reached;'.format(
                    network=self.network)
                self.run_query(sql)
                return

        msg = 'No Vertex has been found that is accessible at least by at least {n:0.0f}% of the links in the network'
        raise ValueError(msg.format(n=self.links_to_find * 100))

    def update_pgr_driving_distance(self, startvertex=1, maxcosts=10000000):
        """
        unreached nodes
        """
        sql = """

CREATE OR REPLACE VIEW "{network}".reached_from AS
-- Knoten, die in Hinrichtung erreicht werden
SELECT seq, node::integer, agg_cost AS cost
FROM pgr_drivingDistance(
E'SELECT id, source, target, cost, reverse_cost
FROM "{network}".edge_table',
{startvertex}, {maxcosts}, true
);

CREATE OR REPLACE VIEW "{network}".reached_to AS
-- Knoten, die in Hinrichtung erreicht werden
SELECT seq, node::integer, agg_cost AS cost
FROM pgr_drivingDistance(
E'SELECT id, source, target, reverse_cost as cost, cost as reverse_cost
 FROM "{network}".edge_table',
{startvertex}, {maxcosts}, true
);
""".format(startvertex=startvertex, maxcosts=maxcosts, network=self.network)
        self.run_query(sql)

    def copy_edge_reached_with_planned(self):
        """
        copy edge_reached into new table
        """
        self.logger.info(
            'Save edges reached with planned roads to table edges_reached')

        sql = """
TRUNCATE "{network}".edges_reached_with_planned;
INSERT INTO "{network}".edges_reached_with_planned
  (id, fromnode, tonode, wayid, segment)
SELECT e.id, e.fromnode, e.tonode, e.wayid, e.segment
FROM "{network}".edges_reached e;
            """.format(network=self.network)
        self.run_query(sql)


if __name__ == '__main__':

    parser = ArgumentParser(description="Extract Data for Model")

    parser.add_argument("-n", '--name', action="store",
                        help="Name of destination database",
                        dest="db", default='extract')

    parser.add_argument('--chunksize', action="store",
                        help="size of chunks in which links are created",
                        type=int,
                        dest="chunksize", default=1000)

    parser.add_argument('--limit', action="store",
                        help="limit the number of links to be created (multiplied by chunksize)",
                        type=str,
                        dest="limit", default='NULL')

    parser.add_argument('--host', action="store",
                        help="host",
                        dest="host", default='gis.ggr-planung.de')
    parser.add_argument("-p", '--port', action="store",
                        help="port", type=int,
                        dest="port", default=5432)
    parser.add_argument("-U", '--user', action="store",
                        help="database user",
                        dest="user", default='osm')

    parser.add_argument("--links-to-find", action="store",
                        help="share of links to find", type=float,
                        dest="links_to_find", default=0.25)

    parser.add_argument("--corine", action="store",
                        help="corine landuse table",
                        dest="corine", default='clc18')

    options = parser.parse_args()

    build_network = BuildNetwork(schema='osm',
                                 network_schema='network',
                                 db=options.db,
                                 limit=options.limit,
                                 chunksize=options.chunksize,
                                 links_to_find=options.links_to_find,
                                 corine=corine)
    build_network.set_login(host=options.host,
                            port=options.port,
                            user=options.user)

    build_network.build()
