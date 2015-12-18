#!/usr/bin/env python
#coding:utf-8

import numpy as np
import logging
logger = logging.getLogger()
logger.addHandler(logging.StreamHandler())
logger.level = logging.DEBUG
import os
import datetime
from extractiontools.connection import Connection, DBApp


class Hafas2GTFS(DBApp):
    def __init__(self, schema='schleswig_flensburg',
                 day=25, month=11, year=2014):
        self.schema = schema
        self.today = datetime.datetime(year, month, day)

    @property
    def day(self):
        return self.today.date().day

    def convert(self):
        with Connection() as conn:
            self.conn = conn
            self.set_search_path()

            ### GTFS
            self.fill_gtfs_stops()
            self.identify_kreis()
            self.line_type()
            self.make_routes()
            self.make_agencies()
            self.make_shapes()
            self.make_stop_times()

            #self.mark_abfahrten_around_kreis()


            self.conn.commit()
            pass

    def set_search_path(self):
        sql = 'SET search_path TO %s, "$user", public;' % self.schema
        cur = self.conn.cursor()
        cur.execute(sql)

    def show_search_path(self):
        cur = self.conn.cursor()
        sql = 'show search_path ;'
        cur.execute(sql)
        rows = cur.fetchall()
        print rows

    def fill_gtfs_stops(self):
        """
        """
        cur = self.conn.cursor()

        # Stops
        sql = """

TRUNCATE gtfs_stops;

INSERT INTO gtfs_stops
SELECT
  h."H_ID" AS stop_id,
  h."H_Name" AS stop_name,
  st_y(h.geom) AS stop_lat,
  st_x(h.geom) AS stop_lon
FROM
  haltestellen AS h
;

-- Generiere neuen langen Routennamen aus Fahrnamen -> Fahrtziel
UPDATE abfahrten a
SET route_name_long =
regexp_replace(a."Fahrt_Name", ' +' ::text, ' ' ::text) || ' -> ' ::text ||
                 a."Fahrt_Ziel" ;

        """

        self.run_query(sql)

    def identify_kreis(self):
        """
        Suche Kreis, in dem die meisten Haltestellen einer Linie liegen
        """
        sql = """
-- Kreis, in der die meisten Abfahrten der Linie liegen, generieren
UPDATE abfahrten a
SET kreis=b.kreis, kennz=k.kennz
FROM
(
SELECT h.abfahrt_id, first_value(h.kreis) OVER(PARTITION BY h.abfahrt_id ORDER BY h.cnt DESC) AS kreis
FROM (
SELECT f.abfahrt_id, h.kreis, count(*) cnt
FROM fahrten f, haltestellen h
WHERE h."H_ID" = f.stop_id
GROUP BY f.abfahrt_id, h.kreis) h
)b LEFT JOIN d.kr_2010 k ON (k.rs = b.kreis)
WHERE a.abfahrt_id = b.abfahrt_id
;
"""
        self.run_query(sql)

    def mark_abfahrten_around_kreis(self, ags=None, kennz=None, bbox=None, ):
        """
        ist nocht nicht fertig
        """
        sql = """
-- Checke, ob Abfahrt im 5 km Radius den Kreis berührt.
UPDATE abfahrten a
SET touches_kreis=TRUE
FROM
(
SELECT DISTINCT f.abfahrt_id
FROM fahrten f, haltestellen h,
d.kr_2010 k
WHERE h."H_ID" = f.stop_id
AND st_dwithin(st_transform(h.geom, 31467), k.geom, 5000)
AND (k.kennz = 'SE' )) b
WHERE a.abfahrt_id = b.abfahrt_id
;

-- nur Routen im Kreis
UPDATE routes r
SET touches_kreis = TRUE
FROM(SELECT DISTINCT route_id FROM abfahrten
WHERE touches_kreis
) b
WHERE r.route_id = b.route_id;
        """

        raise NotImplementedError('Funktion ist noch nicht fertig')

    def line_type(self):
        """
        """
        sql = """

-- Linientyp suchen
UPDATE abfahrten a
SET typ = regexp_replace(a."Fahrt_Name", ' *[0-9]+','');

UPDATE abfahrten a
SET typ = substr(typ,1,3)
WHERE a.typ ~ '^Bus|AST|Fäh|NWB *';

-- route_short_name
UPDATE abfahrten
SET route_short_name = regexp_replace("Fahrt_Name", '^'||typ||' *','');

-- agency_name
UPDATE abfahrten
SET agency_name = kennz||' '||typ
WHERE typ = 'Bus' or typ = 'AST' or typ = 'ALT' or typ = 'Fäh'
or typ = 'U' or typ = 'Str' or typ = 'Schiff' or typ = 'S';
UPDATE abfahrten
SET agency_name = typ
WHERE agency_name is NULL;

        """
        self.run_query(sql)

    def make_routes(self):
        """
        """
        sql = """
TRUNCATE routes;
INSERT INTO routes (route_id, agency_name, typ, route_short_name, abfahrten_list, h_folge, fz_folge, hz_folge)
SELECT *
FROM
(
SELECT row_number() OVER(ORDER BY agency_name, route_short_name)::integer AS route_id,
agency_name, typ, route_short_name, abfahrten_list,
h_folge, fz_folge, hz_folge
FROM
(
SELECT a.agency_name, a.typ, a.route_short_name, array_accum(a.abfahrt_id) AS abfahrten_list,
h_folge, fz_folge, hz_folge
FROM
(SELECT
a.abfahrt_id, a.agency_name, a.typ, a.agency_name||'_'||a.route_short_name AS route_short_name,
ff.h_folge, ff.fz_folge, ff.hz_folge
FROM
(SELECT f.abfahrt_id,
array_accum(stop_id) as h_folge,
array_accum("H_Ankunft" - lag_abfahrt) AS fz_folge,
array_accum("H_Abfahrt" - "H_Ankunft") AS hz_folge
FROM (
SELECT abfahrt_id,stop_id, "H_Ankunft", "H_Abfahrt", fahrt_index,
lag("H_Abfahrt") OVER(PARTITION BY abfahrt_id ORDER BY fahrt_index) AS lag_abfahrt
FROM
  fahrten f
ORDER BY abfahrt_id, fahrt_index)f
GROUP BY abfahrt_id
) ff, abfahrten a
WHERE ff.abfahrt_id = a.abfahrt_id ) a
GROUP BY
a.agency_name, a.typ, a.route_short_name, h_folge, fz_folge, hz_folge
) r
) r;

-- Routen den abfahrten zuweisen
UPDATE abfahrten a
SET route_id = r.route_id
FROM routes r
WHERE a.abfahrt_id = ANY(r.abfahrten_list)
;
        """
        self.run_query(sql)

    def make_agencies(self):
        """
        """
        sql = """
-- agency-Tabelle erzeugen
TRUNCATE agencies;
INSERT INTO agencies (agency_id, agency_name)
SELECT row_number() OVER(ORDER BY typ, agency_name)::integer AS agency_id, agency_name
FROM (SELECT DISTINCT typ, agency_name FROM routes) r;

TRUNCATE gtfs_agency;
INSERT INTO gtfs_agency (agency_id, agency_name)
SELECT agency_id, agency_name
FROM agencies;


-- Langen Liniennamen generieren
UPDATE routes r
SET route_long_name = r.agency_name || '_' || r.route_short_name || ' -> ' || h."H_Name"
FROM haltestellen h
WHERE h."H_ID" = h_folge[array_upper(h_folge, 1)]
;
UPDATE routes r
SET route_long_name = r.agency_name || '_' || r.route_short_name
WHERE route_long_name IS NULL;
        """

        self.run_query(sql)

    def make_shapes(self):
        """
        """
        sql = """

-- generate shapes
TRUNCATE shapes;
INSERT INTO shapes
SELECT row_number() OVER(ORDER BY h_folge) AS shape_id, h_folge
FROM(
SELECT DISTINCT h_folge from routes
--WHERE touches_kreis
) r;

UPDATE routes r
SET shape_id = s.shape_id
FROM shapes s
WHERE s.h_folge = r.h_folge;




TRUNCATE gtfs_routes;
INSERT INTO gtfs_routes
(route_id, agency_id, route_short_name, route_long_name,
route_type)
SELECT
r.route_id, a.agency_id, r.route_short_name, r.route_long_name,
t.route_type
FROM routes r
LEFT JOIN public.route_types t ON (r.typ = t.typ),
agencies a
WHERE a.agency_name = r.agency_name
--AND r.touches_kreis
;


-- alle anderen sind Züge
UPDATE gtfs_routes
SET route_type = 2 WHERE route_type IS NULL;


TRUNCATE gtfs_shapes;
INSERT INTO gtfs_shapes
(shape_id, shape_pt_sequence, shape_pt_lon, shape_pt_lat)
SELECT shape_id::text,
shape_pt_sequence,
st_x(h.geom) AS shape_pt_lon, st_y(h.geom) AS shape_pt_lat
FROM
(SELECT shape_id, unnest(h_folge) AS h_id, generate_subscripts(h_folge, 1) AS shape_pt_sequence
FROM shapes s) r3,
haltestellen h
WHERE r3.h_id = h."H_ID"
ORDER BY shape_id, shape_pt_sequence;
-- delete invalid points
DELETE FROM gtfs_shapes
WHERE shape_pt_lon = 0 AND shape_pt_lat = 0;

UPDATE routes r
SET geom = st_setsrid(st_makeline(points), 4326)
FROM (
SELECT shape_id, st_accum(point) AS points
FROM (SELECT s.shape_id, st_makepoint(s.shape_pt_lon, s.shape_pt_lat) AS point
FROM gtfs_shapes s
ORDER BY s.shape_id, s.shape_pt_sequence) s
GROUP BY s.shape_id
) s
WHERE s.shape_id = r.shape_id
AND array_upper(points,1) > 1;


TRUNCATE gtfs_trips;
INSERT INTO gtfs_trips (
    route_id,
    service_id,
    trip_id, shape_id)
SELECT r.route_id, 1 AS service_id, a.abfahrt_id AS trip_id, r.shape_id
FROM routes r, abfahrten a
WHERE a.route_id = r.route_id
--AND r.touches_kreis
;
        """
        self.run_query(sql)

    def make_stop_times(self):
        """
        """
        sql = """

TRUNCATE gtfs_stop_times;
INSERT INTO gtfs_stop_times
(trip_id, arrival_time, departure_time,
stop_id, stop_sequence, pickup_type, drop_off_type)

SELECT f.abfahrt_id AS trip_id,
CASE
WHEN date_part('day', "H_Ankunft")= {today} THEN to_char("H_Ankunft", 'HH24:MI:SS')
ELSE (date_part('hour', "H_Ankunft") + 24)::text || to_char("H_Ankunft", ':MI:SS')
END AS arrival_time,
CASE
WHEN date_part('day', "H_Abfahrt")= {today} THEN to_char("H_Abfahrt", 'HH24:MI:SS')
ELSE (date_part('hour', "H_Abfahrt") + 24)::text || to_char("H_Abfahrt", ':MI:SS')
END AS departure_time,
f.stop_id, --::text,
f.fahrt_index AS stop_sequence,
CASE
WHEN f."H_Abfahrt" IS NULL THEN 1
WHEN a.typ = 'AST' or a.typ = 'ALT' THEN 2
ELSE 0
END AS pickup_type,
CASE
WHEN f."H_Ankunft" IS NULL THEN 1
WHEN a.typ = 'AST' or a.typ = 'ALT' THEN 2
ELSE 0
END AS drop_off_type

FROM fahrten f, abfahrten a
WHERE f.abfahrt_id = a.abfahrt_id
--AND a.touches_kreis
;

-- replace NULL values in arrival and departure time
UPDATE gtfs_stop_times
SET arrival_time = departure_time
WHERE arrival_time IS NULL;

UPDATE gtfs_stop_times
SET departure_time = arrival_time
WHERE departure_time IS NULL;
        """.format(today=self.day)
        self.run_query(sql)

    def export_gtfs(self, path=''):
        """
        exports the data to the path
        """
        with Connection() as conn:
            self.conn = conn
            self.set_search_path()
            cur = self.conn.cursor()
            sql = '''SET CLIENT_ENCODING TO '{encoding}';'''
            encoding = 'LATIN9'
            cur.execute(sql.format(encoding=encoding))
            tables = ['stops', 'agency', 'stop_times', 'routes', 'trips', 'shapes',
                      'calendar', 'calendar_dates', 'transfers']
            for table in tables:
                tn = 'gtfs_{tn}'.format(tn=table)
                fn = os.path.join(path, '{tn}.txt'.format(tn=table))
                with open(fn, 'w') as f:
                    sql = self.conn.copy_sql.format(tn=tn, fn=fn)
                    logger.info(sql)
                    cur.copy_expert(sql, f)


            sql = '''RESET CLIENT_ENCODING;'''
            cur.execute(sql)


if __name__=='__main__':


    parser = ArgumentParser(description="Convert Hafas-Data to GTFS")

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
    parser.add_argument("-D", '--date', action="store",
                        help="database user",
                        dest="date")
    parser.add_argument('--source-db', action="store",
                        help="source database",
                        dest="source_db", default='dplus')

    options = parser.parse_args()

    hafas = Hafas2GTFS(date=options.date)
    hafas.set_login(host=options.host, port=options.port, user=options.user)
    hafas.convert()
    #hafas.export_gtfs(path=r'W:\\mobil\66 Zwischenablage Ben\GTFS_Schleswig_Flensburg\GTFS_neu')
