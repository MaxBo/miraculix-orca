#!/usr/bin/env python
#coding:utf-8

from argparse import ArgumentParser

from extractiontools.build_network_car import BuildNetwork

class BuildNetworkWalkCycle(BuildNetwork):
    """
    Build Network Analyst-Data in the target Database...
    """

    def create_roads(self):
        """
        Create the view for all ways suided for walking and cycling
        """

        sql = """
CREATE MATERIALIZED VIEW {network}.roads AS
SELECT
id, bool_or(a.foot) AS foot, bool_or(a.bicycle) AS bicycle
FROM
(SELECT
w.id,
(lt.foot OR COALESCE(at.oeffne_walk, false)) AND NOT COALESCE(at.sperre_walk, false) AS foot,
(lt.bicycle OR COALESCE(at.oeffne_bike, false)) AND NOT COALESCE(at.sperre_bike, false) AS bicycle
FROM
{network}.streets s,
classifications.linktypes lt,
osm.ways AS w
LEFT JOIN classifications.access_walk_cycle AS at ON w.tags @> at.tags
WHERE s.id = w.id
AND s.linktype_id = lt.id
) a
GROUP BY a.id
HAVING bool_or(a.foot) or bool_or(a.bicycle);
""".format(network=self.network)
        self.run_query(sql)

    def create_links(self):
        """
        create links
        """
        sql = """
DROP TABLE IF EXISTS {network}.links;
CREATE TABLE {network}.links
(
  fromnode bigint,
  tonode bigint,
  wayid bigint NOT NULL,
  segment integer NOT NULL,
  geom geometry(LINESTRING, {srid}),
  linkname text,
  linkref text,
  linktype integer,
  v_foot_hin double precision,
  v_foot_rueck double precision,
  v_bicycle_hin double precision,
  v_bicycle_rueck double precision,
  io boolean NOT NULL DEFAULT false,
  t_foot_hin double precision,
  t_foot_rueck double precision,
  t_bicycle_hin double precision,
  t_bicycle_rueck double precision,
  oneway boolean NOT NULL DEFAULT false,
  planned boolean NOT NULL DEFAULT false,
  construction boolean NOT NULL DEFAULT false,
  bridge_tunnel "char" NOT NULL DEFAULT ''::"char",
  slope double precision DEFAULT 0
)
WITH (
  OIDS=FALSE
);
        """
        self.run_query(sql.format(srid=self.srid, network=self.network))

    def update_oneway(self):
        """

        """
        sql = """
UPDATE {network}.links l
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

UPDATE {network}.links l
SET oneway = FALSE
FROM osm.ways w
WHERE
  w.tags -> 'oneway' = 'no'
AND w.id = l.wayid
;

-- drehe links mit oneway = -1 und setze oneway auf True
UPDATE {network}.links l
SET oneway = TRUE,
    fromnode = tonode,
    tonode = fromnode,
    geom = st_reverse(geom)
FROM osm.ways w
WHERE
  w.tags -> 'oneway' = '-1'
AND w.id = l.wayid
;

UPDATE {network}.links l
SET oneway = FALSE
FROM osm.ways w,
classifications.cycling_attributes ca
WHERE ca.opposite = true
AND l.wayid = w.id
AND w.tags @> ca.tags
;

        """.format(network=self.network)
        self.run_query(sql)

    def update_speed(self):
        """
        """
        sql = """

CREATE OR REPLACE FUNCTION {network}.calc_v_rad (slope double precision)
RETURNS double precision
---berechne Geschwindigkeit in Abhängigkeit von Hin und Rückrichtung, vRad ist gedeckelt auf 30 km/h
AS $$
import numpy as np
if slope == 0:
    v = 15.119693 # Geschwindigkeit bei Steigung 0
else:
    x = slope + 0.09
    v = (4.02-213*(x-0.2)-449*(x**3-0.008)-1412*(x**3*np.log(x)+0.0128))*(slope >= -0.08 and slope <.30) + \
    (.4/slope) * (slope >= 0.30)+ 30*(slope < -0.08)
    if np.isnan(v):
        v=30
return v
$$ LANGUAGE plpythonu;

CREATE OR REPLACE FUNCTION {network}.calc_v_fuss (slope double precision)
RETURNS double precision
---berechne Geschwindigkeit in Abhängigkeit von Hin und Rückrichtung, vFuss ist gedeckelt auf 7 km/h
---bei Gefälle steigt die Geschwindigkeit zunächst bis auf 7 km/h und nimmt dann aber wieder ab bis auf 3.5 km/h, da man berab auch nicht so schnell gehen kann
AS $$
import numpy as np
v0 = 5.
x = slope
if slope == 0:
    v = v0 # noch mal überprüfen
else:
    v = (100 * x**3 - 30*x**2 - 13*x + v0) * (slope <.30 and slope >-.30) +\
    (.33/slope) * (slope >=.30) + 3.5 * (slope <=-.30)
return v
$$ LANGUAGE plpythonu;
        """.format(network=self.network)
        self.run_query(sql)

        sql = """
DROP AGGREGATE IF EXISTS {network}.mul(double precision) CASCADE;
CREATE AGGREGATE {network}.mul(double precision)
( SFUNC = float8mul, STYPE=double precision);

UPDATE {network}.links l
SET
v_foot_hin = {network}.calc_v_fuss(l.slope),
v_foot_rueck = {network}.calc_v_fuss(-l.slope),
v_bicycle_hin = {network}.calc_v_rad(l.slope) * bf.factor,
v_bicycle_rueck = {network}.calc_v_rad(-l.slope) * bf.factor
FROM
(
SELECT
l.wayid,
l.segment,
lt.surroundingfactor * COALESCE(ls.factor, 1) AS factor
FROM
classifications.linktypes lt,
{network}.links l LEFT JOIN
(SELECT
wayid, segment, {network}.mul(factor) AS factor
FROM (
SELECT
l.wayid, l.segment, ca.factor
FROM
osm.ways w,
{network}.links l,
classifications.cycling_attributes ca
WHERE l.wayid = w.id
AND w.tags @> ca.tags
) ll
GROUP BY wayid, segment
) ls ON (ls.wayid = l.wayid AND ls.segment = l.segment)
WHERE l.linktype = lt.id) bf
WHERE bf.wayid = l.wayid AND bf.segment = l.segment
;
        """.format(network=self.network)
        self.run_query(sql)

    def update_time(self):
        """

        """
        sql = """
UPDATE {network}.links l
SET
  t_foot_hin = st_length(l.geom) / l.v_foot_hin * 3.6 / 60,
  t_foot_rueck = st_length(l.geom) / l.v_foot_rueck * 3.6 / 60,
  t_bicycle_hin = st_length(l.geom) / l.v_bicycle_hin * 3.6 / 60,
  t_bicycle_rueck = st_length(l.geom) / l.v_bicycle_rueck * 3.6 / 60
;

WITH g AS
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
 {network}.links l,
 classifications.linktypes lt,
 osm.ways w
WHERE lt.road_category = 'D'
AND w.tags ? 'duration'
AND l.wayid = w.id AND l.linktype = lt.id) f
)
UPDATE {network}.links l
SET
  t_foot_hin = g.duration,
  t_foot_rueck = g.duration,
  t_bicycle_hin = g.duration,
  t_bicycle_rueck = g.duration
FROM
 g
WHERE l.wayid = g.wayid AND l.segment = g.segment;
        """.format(network=self.network)
        self.run_query(sql)

    def update_lanes(self):
        """
        """

    def create_barriers(self):
        """
        Create Barriers
        """
        sql = """
CREATE OR REPLACE VIEW {network}.barriers_foot AS
 SELECT b.id,
    b.geom,
    COALESCE(b.closed, false) AS explicitly_closed,
    b.tags -> 'barrier'::text AS barrier_type,
    b.tags -> 'note'::text AS note,
    b.tags
   FROM ( SELECT n.id,
            n.tags,
            bool_or(a.sperre_walk) AS closed,
            bool_or(a.oeffne_walk) AS opened,
            n.geom
           FROM {network}.link_points lp,
            osm.nodes n
             LEFT JOIN classifications.access_walk_cycle a ON n.tags @> a.tags
          WHERE n.id = lp.nodeid AND n.tags ? 'barrier'::text
          GROUP BY n.id
         HAVING bool_or(a.sperre_walk) OR (bool_or(a.oeffne_walk) IS NULL)
         )b
;
CREATE OR REPLACE VIEW {network}.barriers_cycle AS
 SELECT b.id,
    b.geom,
    COALESCE(b.closed, false) AS explicitly_closed,
    b.tags -> 'barrier'::text AS barrier_type,
    b.tags -> 'note'::text AS note,
    b.tags
   FROM ( SELECT n.id,
            n.tags,
            bool_or(a.sperre_bike) AS closed,
            bool_or(a.oeffne_bike) AS opened,
            n.geom
           FROM {network}.link_points lp,
            osm.nodes n
             LEFT JOIN classifications.access_walk_cycle a ON n.tags @> a.tags
          WHERE n.id = lp.nodeid AND n.tags ? 'barrier'::text
          GROUP BY n.id
         HAVING bool_or(a.sperre_bike) OR (bool_or(a.oeffne_bike) IS NULL)
         ) b
;
        """.format(network=self.network)
        self.run_query(sql)

    def update_egde_table(self):
        """
        Updates the edge_table
        """
        if self.options.routing_walk:
            cost = 'l.t_foot_hin'
            reverse_cost = 'l.t_foot_rueck'
        else:
            cost = 'l.t_bicycle_hin'
            reverse_cost = 'CASE WHEN l.oneway THEN -1 ELSE l.t_bicycle_rueck END'


        sql = """

TRUNCATE {network}.edge_table;
INSERT INTO {network}.edge_table (id, fromnode, tonode, geom,
cost, reverse_cost)
SELECT
  row_number() OVER (ORDER BY fromnode, tonode)::integer AS id,
  fromnode,
  tonode,
  l.geom,
  {cost} AS cost,
  {reverse_cost} AS reverse_cost
FROM {network}.links l;
        """.format(network=self.network, cost=cost, reverse_cost=reverse_cost)
        self.run_query(sql)

    def create_views_roadtypes(self):
        """
        """
        sql = """
-- erstelle View für die einzelnen Streckentypen
CREATE OR REPLACE VIEW {network}.walk_cycle_network AS
SELECT l.*
FROM
  {network}.links_reached_without_planned l;

CREATE OR REPLACE VIEW {network}.walk_cycle_network_only_by_planned AS
SELECT l.*
FROM
  {network}.links_reached_only_by_planned l;
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
                        dest="user", default='max')

    parser.add_argument("--links-to-find", action="store",
                        help="share of links to find", type=float,
                        dest="links_to_find", default='0.5')

    parser.add_argument("--routing_walk", action='store_true',
                        help='routing for walking (cycling is the default',
                        dest='routing_walk', default=False)


    options = parser.parse_args()

    build_network = BuildNetworkWalkCycle(schema='osm',
                                 network_schema='network_fr',
                                 db=options.db,
                                 options=options,)
    build_network.set_login(host=options.host,
                            port=options.port,
                            user=options.user)

    build_network.build()