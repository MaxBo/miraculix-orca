#!/usr/bin/env python
# coding:utf-8

import datetime
import os
import zipfile
from argparse import ArgumentParser
import orca
from extractiontools.connection import Connection
from extractiontools.ausschnitt import Extract
from extractiontools.utils.get_date import Date


class HafasDB2GTFS(Extract):
    """Convert Hafas to GTFS"""

    schema = 'timetables'

    def __init__(self,
                 db: str,
                 date: datetime.date,
                 only_one_day: bool,
                 base_path: str,
                 subfolder: str,
                 tbl_kreise: str,
                 logger=None
                 ):
        self.logger = logger or orca.logger
        self.destination_db = db
        self.set_login(database=self.destination_db)
        self.target_srid = self.get_target_srid()
        self.today = Date(date.year, date.month, date.day)
        self.only_one_day = only_one_day
        self.base_path = base_path
        self.subfolder = subfolder
        self.tbl_kreise = f'verwaltungsgrenzen.{tbl_kreise}'

    def convert(self):
        with Connection(self.login) as conn:
            self.conn = conn
            self.set_search_path()
            self.create_aggregate_functions()
            self.create_timetable_tables()
            self.create_routes_tables()
            self.create_gtfs_tables()
            self.truncate_routes_tables()
            self.truncate_gtfs_tables()
            self.count()
            self.delete_invalid_fahrten()
            self.set_stop_id()
            self.write_calendar()
            # self.workaround_sommerzeit()
            self.test_for_negative_travel_times()
            self.set_arrival_to_day_before()
            self.set_departure_to_day_before()
            self.shift_trips()
            self.test_for_negative_travel_times()
            self.set_ein_aus()

            self.count_multiple_abfahrten()
            # self.show_multiple_trips()
            self.save_haltestellen_id()
            self.delete_multiple_abfahrten()

            # Haltestellen
            # self.update_haltestellen_d()

            self.reset_stop_id()
            self.conn.commit()
            self.set_eindeutige_stop_id()
            self.conn.commit()
            self.update_stop_id_from_database()
            self.conn.commit()

            # self.cleanup_haltestellen()

            self.update_stop_id_from_similar_routes()
            self.intersect_kreise(self.tbl_kreise)

            # GTFS
            self.fill_gtfs_stops()
            self.identify_kreis(self.tbl_kreise)
            self.line_type()
            self.make_routes()
            self.make_agencies()
            self.make_shapes()
            self.make_stop_times()
            # self.mark_abfahrten_around_kreis()
            self.conn.commit()

    def create_gtfs_tables(self):
        """Create the gtfs tables if not yet exists"""
        self.logger.info(f'Creating gtfs tables')
        sql = """
CREATE TABLE IF NOT EXISTS gtfs_agency (
  agency_id TEXT NOT NULL,
  agency_name TEXT,
  agency_url TEXT DEFAULT 'www.ggr-planung.de'::text,
  agency_timezone TEXT DEFAULT 'Europe/Berlin'::text NOT NULL,
  CONSTRAINT gtfs_agency_pkey PRIMARY KEY(agency_id)
);

 CREATE TABLE IF NOT EXISTS gtfs_calendar (
  service_id TEXT,
  monday INTEGER,
  tuesday INTEGER,
  wednesday INTEGER,
  thursday INTEGER,
  friday INTEGER,
  saturday INTEGER,
  sunday INTEGER,
  start_date TEXT,
  end_date TEXT
) ;

CREATE TABLE IF NOT EXISTS gtfs_calendar_dates (
  service_id TEXT,
  date TEXT,
  exception_type INTEGER
) ;

CREATE TABLE IF NOT EXISTS gtfs_frequencies (
  trip_id TEXT,
  start_time INTEGER,
  end_time INTEGER,
  headway_secs INTEGER
) ;
CREATE TABLE IF NOT EXISTS gtfs_routes (
  agency_id TEXT,
  route_id TEXT NOT NULL,
  route_short_name TEXT,
  route_long_name TEXT,
  route_type INTEGER,
  CONSTRAINT gtfs_routes_pkey PRIMARY KEY(route_id)
) ;
CREATE TABLE IF NOT EXISTS gtfs_shapes (
  shape_id TEXT NOT NULL,
  shape_pt_lat DOUBLE PRECISION,
  shape_pt_lon DOUBLE PRECISION,
  shape_pt_sequence INTEGER NOT NULL,
  shape_dist_traveled DOUBLE PRECISION,
  CONSTRAINT gtfs_shapes_idx PRIMARY KEY(shape_id, shape_pt_sequence)
) ;

CREATE TABLE IF NOT EXISTS gtfs_stop_times (
  trip_id TEXT NOT NULL,
  arrival_time TEXT,
  departure_time TEXT,
  stop_id TEXT,
  stop_sequence INTEGER NOT NULL,
  shape_dist_traveled DOUBLE PRECISION,
  pickup_type SMALLINT,
  drop_off_type SMALLINT,
  CONSTRAINT gtfs_stop_times_stop_times_trip_id PRIMARY KEY(trip_id, stop_sequence)
) ;

CREATE INDEX IF NOT EXISTS gtfs_stop_times_stop_times_stop_id ON gtfs_stop_times
  USING btree (stop_id);

CREATE TABLE IF NOT EXISTS gtfs_stops (
  stop_id TEXT NOT NULL,
  stop_name TEXT,
  stop_lat DOUBLE PRECISION,
  stop_lon DOUBLE PRECISION,
  CONSTRAINT gtfs_stops_pkey PRIMARY KEY(stop_id)
) ;

CREATE INDEX IF NOT EXISTS gtfs_stops_stops_stop_lat ON gtfs_stops
  USING btree (stop_lat);

CREATE INDEX IF NOT EXISTS gtfs_stops_stops_stop_lon ON gtfs_stops
  USING btree (stop_lon);

CREATE TABLE IF NOT EXISTS gtfs_transfers (
  from_stop_id TEXT,
  to_stop_id TEXT,
  transfer_type INTEGER,
  min_transfer_time DOUBLE PRECISION
) ;

CREATE TABLE IF NOT EXISTS gtfs_trips (
  route_id TEXT,
  trip_id TEXT NOT NULL,
  service_id TEXT,
  shape_id TEXT,
  trip_headsign TEXT,
  trip_short_name TEXT,
  CONSTRAINT gtfs_trips_pkey PRIMARY KEY(trip_id)
);

CREATE INDEX IF NOT EXISTS gtfs_trips_trips_route_id ON gtfs_trips
  USING btree (route_id);

        """
        self.run_query(sql)

    def create_routes_tables(self):
        """Create routes and agency tables"""
        self.logger.info(f'Creating route tables')
        sql = """
CREATE TABLE IF NOT EXISTS agencies (
  agency_id INTEGER NOT NULL,
  agency_name TEXT,
  CONSTRAINT agencies_wilster_pkey PRIMARY KEY(agency_id)
);
CREATE TABLE IF NOT EXISTS routes (
  route_id INTEGER NOT NULL,
  agency_name TEXT,
  route_short_name TEXT,
  abfahrten_list BIGINT[],
  h_folge INTEGER[],
  fz_folge INTERVAL(6)[],
  hz_folge INTERVAL(6)[],
  typ TEXT,
  route_long_name TEXT,
  shape_id TEXT,
  geom geometry(LINESTRING, 4326),
  touches_kreis BOOLEAN DEFAULT false NOT NULL,
  CONSTRAINT routes_pkey PRIMARY KEY(route_id)
);

CREATE INDEX IF NOT EXISTS routes_linestring_idx ON routes
  USING gist (geom);

 CREATE TABLE IF NOT EXISTS shapes (
  shape_id INTEGER NOT NULL,
  h_folge INTEGER[],
  CONSTRAINT shapes_pkey PRIMARY KEY(shape_id)
) ;
        """
        self.run_query(sql)

    def truncate_gtfs_tables(self):
        """Truncate the gtfs tables"""
        sql = """
TRUNCATE gtfs_agency;
TRUNCATE gtfs_calendar;
TRUNCATE gtfs_calendar_dates;
TRUNCATE gtfs_frequencies;
TRUNCATE gtfs_routes;
TRUNCATE gtfs_shapes;
TRUNCATE gtfs_stop_times;
TRUNCATE gtfs_stops;
TRUNCATE gtfs_transfers;
TRUNCATE gtfs_trips;
        """
        self.run_query(sql)

    def truncate_routes_tables(self):
        """Truncate the routes tables"""
        sql = """
TRUNCATE agencies;
TRUNCATE routes;
TRUNCATE shapes;
        """
        self.run_query(sql)

    def create_timetable_tables(self):
        """
        make a backup of the timetables
        """
        self.logger.info(f'Creating timetables')
        sql = """
CREATE SEQUENCE IF NOT EXISTS stop_id_seq START 10000000;

CREATE TABLE IF NOT EXISTS {schema}.stops
(
  "H_Name" text,
  "H_ID" integer NOT NULL DEFAULT nextval('stop_id_seq'),
  kreis text,
  found boolean,
  geom geometry(Point, {srid}),
  keep boolean NOT NULL DEFAULT true,
  CONSTRAINT stops_pkey PRIMARY KEY ("H_ID")
);

CREATE INDEX IF NOT EXISTS stops_geom_idx ON stops
  USING gist (geom);

CREATE TABLE IF NOT EXISTS {schema}.departures
(
  "Fahrt_Name" text,
  "Fahrt_Abfahrt" timestamp(0) with time zone,
  "H_ID" integer,
  abfahrt_id bigint NOT NULL,
  "Fahrt_Ziel" text,
  abfahrt_id_final integer,
  keep boolean,
  route_name_long text,
  kreis text,
  kennz text,
  typ text,
  route_short_name text,
  agency_name text,
  route_id integer,
  CONSTRAINT departures_pkey PRIMARY KEY (abfahrt_id),
  CONSTRAINT departures_fk FOREIGN KEY ("H_ID")
      REFERENCES {schema}.stops ("H_ID") MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE NO ACTION,
  CONSTRAINT departures_fk1 FOREIGN KEY (abfahrt_id_final)
      REFERENCES {schema}.departures (abfahrt_id) MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE NO ACTION
);

CREATE TABLE IF NOT EXISTS {schema}.trips
(
  "Fahrt_Name" text,
  "H_Ankunft" timestamp(0) with time zone,
  "H_Abfahrt" timestamp(0) with time zone,
  "H_Name" text,
  abfahrt_id bigint NOT NULL,
  fahrt_index integer NOT NULL,
  "H_ID" integer,
  h_id_backup integer,
  stop_id integer,
  stop_id_txt text,
  ein boolean DEFAULT true NOT NULL,
  aus boolean DEFAULT true NOT NULL,
  CONSTRAINT trips_idx PRIMARY KEY (abfahrt_id, fahrt_index),
  CONSTRAINT trips_fk FOREIGN KEY (abfahrt_id)
      REFERENCES {schema}.departures (abfahrt_id) MATCH SIMPLE
      ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS trips_idx1
  ON timetables.trips
  USING btree
  ("H_Name" COLLATE pg_catalog."default", "H_Abfahrt");

CREATE INDEX IF NOT EXISTS trips_hid_idx
  ON timetables.trips
  USING btree
  ("H_ID");

CREATE INDEX IF NOT EXISTS trips_hid_idx
  ON timetables.trips
  USING btree
  ("stop_id_txt");

CREATE INDEX IF NOT EXISTS trips_no_stop_id_idx
  ON timetables.trips
  USING btree
  ("stop_id")
  WHERE stop_id IS NULL;

TRUNCATE {schema}.stops CASCADE;
TRUNCATE {schema}.departures CASCADE;
TRUNCATE {schema}.trips CASCADE;

INSERT INTO {schema}.stops ("H_Name", "H_ID", found, geom)
SELECT h."H_Name", h."H_ID", false::boolean, geom
FROM {schema}.haltestellen h;

INSERT INTO {schema}.departures
("Fahrt_Name", "Fahrt_Abfahrt", "H_ID", abfahrt_id, "Fahrt_Ziel")
SELECT
a."Fahrt_Name", a."Fahrt_Abfahrt", a."H_ID", a.abfahrt_id, a."Fahrt_Ziel"
FROM {schema}.abfahrten a;

INSERT INTO {schema}.trips
("Fahrt_Name", "H_Ankunft", "H_Abfahrt", "H_Name",
abfahrt_id, fahrt_index, "H_ID")
SELECT
f."Fahrt_Name", f."H_Ankunft", f."H_Abfahrt", f."H_Name",
f.abfahrt_id, f.fahrt_index, f."H_ID"
FROM {schema}.fahrten f;
        """
        self.run_query(sql.format(schema=self.schema,
                                  srid=self.target_srid))

    def set_search_path(self):
        sql = 'SET search_path TO %s, "$user", public;' % self.schema
        cur = self.conn.cursor()
        cur.execute(sql)

    def show_search_path(self):
        cur = self.conn.cursor()
        sql = 'show search_path ;'
        cur.execute(sql)
        rows = cur.fetchall()
        self.logger.info(rows)

    def create_aggregate_functions(self):
        """
        """
        # check if aggregate function already exists
        sql = '''
SELECT 1
FROM pg_proc p
WHERE
p.pronamespace = 'public'::regnamespace AND
p.proname = 'array_accum' AND
p.prokind = 'a'
AND p.prorettype = 'anycompatiblearray'::regtype::oid
AND p.proargtypes = ARRAY['anycompatible'::regtype]::oidvector
;
        '''
        cur = self.conn.cursor()
        cur.execute(sql)
        # if not exists yet
        if not cur.rowcount:
            sql = """
CREATE AGGREGATE public.array_accum (anycompatible)
(
    sfunc = array_append,
    stype = anycompatiblearray,
    initcond = '{}'
);
        """
            self.run_query(sql)

    def count(self):
        sql = 'SELECT count(*) FROM stops;'
        cur = self.conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        self.logger.info('Haltestellen: %s, ' % rows[0][0])

        sql = 'SELECT count(*) FROM departures;'
        cur = self.conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        self.logger.info('Abfahrten: %s ' % rows[0][0])

        sql = 'SELECT count(*) FROM trips;'
        cur = self.conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        self.logger.info('Fahrten: %s' % rows[0][0])

    def delete_invalid_fahrten(self):
        sql = """
DELETE FROM trips WHERE abfahrt_id IN (
SELECT f.abfahrt_id FROM trips f LEFT JOIN departures a
ON f.abfahrt_id = a.abfahrt_id
WHERE a.abfahrt_id IS NULL) ;
        """
        cur = self.conn.cursor()
        cur.execute(sql)
        self.logger.info('Delete invalid trips: %s' % cur.statusmessage)

    def set_stop_id(self):
        sql = """
UPDATE trips SET stop_id = "H_ID" WHERE "H_ID" IS NOT NULL;
UPDATE trips SET stop_id = NULL WHERE stop_id >= 10000000;
DELETE FROM stops WHERE "H_ID" >= 10000000;
        """
        cur = self.conn.cursor()
        cur.execute(sql)
        self.logger.info('Set Stop ID: %s' % cur.statusmessage)

    def workaround_sommerzeit(self):
        """
        add 1 hour
        """
        sql = '''
UPDATE departures SET
"Fahrt_Abfahrt" = "Fahrt_Abfahrt" + '1 hour'::INTERVAL;
UPDATE trips SET
"H_Ankunft" = "H_Ankunft" + '1 hour'::INTERVAL,
"H_Abfahrt" = "H_Abfahrt" + '1 hour'::INTERVAL;
        '''
        self.run_query(sql)

    def write_calendar(self):
        """
        write calendar and calendar dates
        """
        str_today = '{:%Y%m%d}'.format(self.today)
        str_tomorrow = '{:%Y%m%d}'.format(self.today + datetime.timedelta(1))
        sql = '''
TRUNCATE gtfs_calendar;
TRUNCATE gtfs_calendar_dates;
'''
        self.run_query(sql)
        # if gtfs should be valid only on one day
        if self.only_one_day:
            sql = '''
INSERT INTO gtfs_calendar VALUES(1,0,0,0,0,0,0,0,{today},{tomorrow});
INSERT INTO gtfs_calendar_dates VALUES(1,{today},1);
INSERT INTO gtfs_calendar_dates VALUES(1,{tomorrow},1);
        '''.format(today=str_today, tomorrow=str_tomorrow)
        # otherwise its valid on all days
        else:
            sql = '''
INSERT INTO gtfs_calendar VALUES(1,1,1,1,1,1,1,1,20000101,20991231);
            '''
        self.run_query(sql)

    def set_arrival_to_day_before(self):
        sql = """

-- setze Ankunftstag für Ankunftszeiten zwischen 12:00 und 23:59 auf Vortag
UPDATE trips f
SET
"H_Ankunft" = "H_Ankunft" - interval '1 day'
FROM
--SELECT * FROM trips f,
(SELECT abf.abfahrt_id, abf.d0, abf.h0, ank.d1, ank.h1
FROM
(SELECT abfahrt_id, h1, d1 FROM
(select abfahrt_id,
extract(hour from "H_Ankunft") AS h1,
extract(day from "H_Ankunft") AS d1,
 "H_Ankunft", fahrt_index,
 max(fahrt_index) over (PARTITION BY abfahrt_id) AS maxindex
FROM trips) f1
WHERE fahrt_index = maxindex) ank,
(SELECT
abfahrt_id,
extract(hour from "H_Abfahrt") AS h0,
extract(day from "H_Abfahrt") AS d0
FROM trips
 WHERE fahrt_index = 1
 ) AS abf,
departures a
WHERE abf.abfahrt_id = ank.abfahrt_id
AND a.abfahrt_id = ank.abfahrt_id
-- wenn der Tag gleich ist
AND abf.d0 = ank.d1
-- und die Ankunft an Endhaltestelle vor der Abfahrt an der Starthaltestelle erfolgt
AND ank.h1 < abf.h0
ORDER BY abf.abfahrt_id) b
WHERE f.abfahrt_id = b.abfahrt_id
AND extract(hour from "H_Ankunft") >= 12;
"""
        cur = self.conn.cursor()
        cur.execute(sql)
        self.logger.info('set_arrival_to_day_before: %s' % cur.statusmessage)

    def set_departure_to_day_before(self):
        """

        """
        sql = """
--setze Abfahrtstag für Ankunftszeiten zwischen 12:00 und 23:59 auf Folgetag
UPDATE trips f
SET
"H_Abfahrt" = "H_Abfahrt" - interval '1 day'
FROM
--SELECT * FROM trips f,
(SELECT abf.abfahrt_id, abf.d0, abf.h0, ank.d1, ank.h1
FROM
(SELECT abfahrt_id, h1, d1
FROM (
SELECT
abfahrt_id,
extract(hour from "H_Ankunft") AS h1 ,
extract(day from "H_Ankunft") AS d1, "H_Ankunft",
fahrt_index, max(fahrt_index) over (PARTITION BY abfahrt_id) AS maxindex
FROM trips
) f1
WHERE fahrt_index = maxindex) ank,
(SELECT
abfahrt_id,
extract(hour from "H_Abfahrt") AS h0,
extract(day from "H_Abfahrt") AS d0
FROM trips WHERE fahrt_index = 1
) AS abf,
departures a
WHERE abf.abfahrt_id = ank.abfahrt_id
AND a.abfahrt_id = ank.abfahrt_id
-- wenn der Tag gleich ist
AND abf.d0 = ank.d1
AND abf.d0 = ank.d1
-- und die Ankunft an Endhaltestelle nach der Abfahrt an der Starthaltestelle erfolgt
AND ank.h1 < abf.h0
ORDER BY abf.abfahrt_id) b
WHERE f.abfahrt_id = b.abfahrt_id
AND extract(hour from "H_Abfahrt") >= 12;
"""
        cur = self.conn.cursor()
        cur.execute(sql)
        self.logger.info('set_departure_to_day_before: %s' %
                         cur.statusmessage)

    def shift_trips(self):
        self.logger.info(f'Shifting trips')
        sql1 = """
-- korrigiere Fahrten über Tagesgrenzen hinaus
--verschiebe Fahrten nach vorne, so dass nur Fahrten heute und morgen auftreten
UPDATE trips f
SET "H_Abfahrt" = "H_Abfahrt" + '1 day'::INTERVAL,
"H_Ankunft" = "H_Ankunft" + '1 day'::INTERVAL
FROM (
SELECT DISTINCT abfahrt_id
FROM trips
WHERE date_part('day', "H_Abfahrt") < {today}) v
WHERE f.abfahrt_id = v.abfahrt_id
;
""".format(today=self.today.day)

        sql2 = """
--verschiebe Fahrten nach hinten, so dass nur Fahrten heute und morgen auftreten
UPDATE trips f
SET "H_Abfahrt" = "H_Abfahrt" - '1 day'::INTERVAL,
"H_Ankunft" = "H_Ankunft" - '1 day'::INTERVAL
FROM (
SELECT DISTINCT abfahrt_id
FROM trips
WHERE date_part('day', "H_Ankunft") > {tomorrow}) v
WHERE f.abfahrt_id = v.abfahrt_id
;

""".format(tomorrow=self.today.day + 1)

        cur = self.conn.cursor()
        n_updated = 1
        while n_updated:
            cur.execute(sql1)
            msg = cur.statusmessage
            n_updated = int(msg.split(' ')[1])
            self.logger.info('Shift Trips forward: %s' % cur.statusmessage)

        n_updated = 1
        while n_updated:
            cur.execute(sql2)
            msg = cur.statusmessage
            n_updated = int(msg.split(' ')[1])
            self.logger.info('Shift Trips backward: %s' % cur.statusmessage)

    def reset_ein_aus(self):
        sql1 = """
UPDATE trips
SET "H_Abfahrt" = NULL
WHERE ein = FALSE;

"""
        sql2 = """
UPDATE trips
SET "H_Ankunft" = NULL
WHERE aus = FALSE;

"""
        cur = self.conn.cursor()
        cur.execute(sql1)
        self.logger.info(f'no Entry: {cur.statusmessage}')
        cur.execute(sql2)
        self.logger.info(f'no Exit: {cur.statusmessage}')

    def set_ein_aus(self):
        sql1 = """
UPDATE trips
SET ein = FALSE
WHERE "H_Abfahrt" IS NULL
"""
        sql2 = """
UPDATE trips
SET aus = FALSE
WHERE "H_Ankunft" IS NULL
"""
        cur = self.conn.cursor()
        cur.execute(sql1)
        self.logger.info(f'no Entry: {cur.statusmessage}')
        cur.execute(sql2)
        self.logger.info(f'no Exit: {cur.statusmessage}')

        sql3 = """
UPDATE trips f
SET "H_Abfahrt" = f."H_Ankunft"
FROM
(SELECT abfahrt_id, fahrt_index FROM
(select abfahrt_id,
 "H_Ankunft", "H_Abfahrt", fahrt_index,
 max(fahrt_index) over (PARTITION BY abfahrt_id) AS maxindex
FROM trips) f1
WHERE fahrt_index < maxindex
AND "H_Ankunft" IS NOT NULL
AND "H_Abfahrt" IS NULL) ank
WHERE ank.abfahrt_id = f.abfahrt_id
AND ank.fahrt_index = f.fahrt_index;
        """
        cur.execute(sql3)
        self.logger.info(
            f'Missing Departure Time added : {cur.statusmessage}')

        sql4 = """
UPDATE trips f
SET "H_Ankunft" = "H_Abfahrt"
WHERE fahrt_index > 1
AND "H_Ankunft" IS NULL
AND "H_Abfahrt" IS NOT NULL;
        """
        cur.execute(sql4)
        self.logger.info(f'Missing Arrival Time added : {cur.statusmessage}')

    def test_for_negative_travel_times(self):
        cur = self.conn.cursor()
        self.logger.info(f'Validating travel times')
        sql = """
SELECT count(*) FROM (
SELECT *,
lag("H_Abfahrt") OVER(PARTITION BY abfahrt_id ORDER BY fahrt_index) AS ab0
FROM trips f )f
where "H_Ankunft" < ab0;
        """
        cur.execute(sql)
        result = cur.fetchone()

        sql = """
SELECT count(*) FROM trips f
where "H_Abfahrt" < "H_Ankunft";

        """
        cur.execute(sql)
        result2 = cur.fetchone()
        if result[0] or result2[0]:
            self.logger.warning('''There exist still negative travel times with
            departures before arrivals!''')
            if result[0]:
                sql = """
SELECT abfahrt_id FROM (
SELECT *,
lag("H_Abfahrt") OVER(PARTITION BY abfahrt_id ORDER BY fahrt_index) AS ab0
FROM trips f )f
WHERE "H_Ankunft" < ab0;

                """
                cur.execute(sql)
                abfahrten = np.array(cur.fetchall())
                self.logger.warning(f'{abfahrten}')

            if result2[0]:
                sql = """
SELECT abfahrt_id FROM trips f
WHERE "H_Abfahrt" < "H_Ankunft";

                """
                cur.execute(sql)
                abfahrten = np.array(cur.fetchall())
                self.logger.warning(f'{abfahrten}')
            # raise ValueError('''There exist still negative travel times with
            # departures before arrivals!''')
        else:
            self.logger.info('no negative travel times')

    def count_multiple_abfahrten(self):
        """
        count the number of occurences of abfahrten
        and save into abfahrt_id_final
        """

        self.logger.info(f'Counting duplicate trips')
        cur = self.conn.cursor()
        sql = """
-- zähle die doppelten Fahrten durch
UPDATE departures a
SET abfahrt_id_final = b.abfahrt_id_final,
    keep =(b.rn = 1) ::bool
FROM (
SELECT a.abfahrt_id,
       row_number() OVER(PARTITION BY a."Fahrt_Name", a.sh_name, a.anz_haltestellen, a.sh_ab,
        a.eh_name, a.eh_an, a.zwh_name, a.zwh_an) AS rn,
       first_value(a.abfahrt_id) OVER(PARTITION BY a."Fahrt_Name", a.sh_name,
        a.anz_haltestellen, a.sh_ab, a.eh_name, a.eh_an, a.zwh_name, a.zwh_an) AS
         abfahrt_id_final
FROM (
       SELECT a.abfahrt_id,
              a."Fahrt_Name",
              a."Fahrt_Ziel",
              a."H_ID",
              f."H_ID" AS sh_id,
              f."H_Name" AS sh_name,
              f."H_Abfahrt" AS sh_ab,
              f1.fahrt_index AS anz_haltestellen,
              f1."H_ID" AS eh_id,
              f1."H_Name" AS eh_name,
              f1."H_Ankunft" AS eh_an,
              f2."H_ID" AS zwh_id,
              f2."H_Name" AS zwh_name,
              f2."H_Ankunft" AS zwh_an
       FROM departures a,
            (
              SELECT f.abfahrt_id,
                     f."H_ID",
                     f."H_Ankunft",
                     f."H_Name",
                     f.fahrt_index,
                     row_number() OVER(PARTITION BY f.abfahrt_id
              ORDER BY f.fahrt_index DESC) rn
              FROM trips f
            ) f1, -- letzte Haltestelle der Fahrt row_number() mit ORDER BY fahrt_index descending, also rückwärte

            trips f, -- erste Haltestelle der Fahrt
            trips f2 -- zweite Haltestelle der Fahrt
       WHERE a.abfahrt_id = f.abfahrt_id
             AND a.abfahrt_id = f1.abfahrt_id
             AND a.abfahrt_id = f2.abfahrt_id
             AND f.fahrt_index = 1 -- erste Haltestelle
             AND f1.rn = 1 --letzte Haltestelle

             AND f2.fahrt_index = 2 --auch 2. Haltestelle gleich, damit Ringfahrten (U3 Barmbek-Barmbek) nicht aus versehen gelöscht werden

       ORDER BY a."Fahrt_Name",
                sh_ab,
                sh_name
            ) a
     ) b
WHERE b.abfahrt_id = a.abfahrt_id;
"""
        cur.execute(sql)
        self.logger.info(f'Multiple_abfahrten: {cur.statusmessage}')

        sql = """
-- zähle, wie viele doppelte vorhanden
SELECT keep,
       count(*)
FROM departures
GROUP BY keep;
        """
        cur.execute(sql)
        rows = cur.fetchall()
        msg = 'keep abfahrten: {0.keep}: {0.count}'
        for row in rows:
            pass
            # self.logger.info(msg.format(row))

    def show_multiple_trips(self):
        cur = self.conn.cursor()
        self.logger.info(f'Finding duplicate trips')
        sql = '''
SELECT
f.*,
a.keep
FROM trips f,
(SELECT
  abfahrt_id,
  abfahrt_id_final
FROM departures
WHERE keep = False) a1,
departures a
WHERE f.abfahrt_id = a.abfahrt_id
AND a1.abfahrt_id_final = a.abfahrt_id_final
ORDER BY f."Fahrt_Name", f.abfahrt_id, f.fahrt_index;
'''
        cur.execute(sql)
        rows = cur.fetchall()
        msg = '{0.Fahrt_Name}\t{0.abfahrt_id}\t{0.fahrt_index}\t{0.H_Ankunft} - {0.H_Abfahrt}\t{0.H_ID}\t{0.H_Name}'
        for row in rows:
            m = msg.format(row)
            self.logger.info(m)

    def save_haltestellen_id(self):
        """
        save H_ID into stop_id before deleting a trip
        """
        self.logger.info(f'Saving stop ids')
        cur = self.conn.cursor()
        sql = """
-- sichere vor dem Löschen H_ID, die bei doppelten Fahrten gefunden wurden in Spalte stop_id
UPDATE trips f
SET stop_id = f2."H_ID"
FROM
     (
       SELECT a.abfahrt_id_final,
              f.fahrt_index,
              max(f."H_ID") AS "H_ID"
       FROM trips f,
            departures a
       WHERE f.abfahrt_id = a.abfahrt_id
       GROUP BY a.abfahrt_id_final,
                f.fahrt_index
     ) f2
WHERE f.abfahrt_id = f2.abfahrt_id_final
      AND f.fahrt_index = f2.fahrt_index
      AND f2."H_ID" IS NOT NULL;
        """
        cur.execute(sql)
        self.logger.info(f'save haltestellen_ids: {cur.statusmessage}')

    def update_haltestellen_d(self,
                              haltestellen='haltestellen_mitteleuropa.haltestellen'):
        """
        Aktualisiere Haltestellen_d
        """
        self.logger.info(f'Updating stops')
        sql = """

INSERT INTO haltestellen_d ("H_ID", "H_Name", geom)
SELECT "H_ID", "H_Name", geom
FROM haltestellen_mitteleuropa.haltestellen t
WHERE NOT EXISTS (SELECT 1 FROM haltestellen_d h WHERE h."H_ID" = t."H_ID");

UPDATE haltestellen_d hd
SET "H_Name" = h."H_Name"
FROM {haltestellen} h
WHERE h."H_ID" = hd."H_ID";

DELETE FROM haltestellen_d h
USING
 (
SELECT hd."H_ID" AS id0, hd."H_Name" AS name0, hv."H_ID" AS id1, hv."H_Name" AS name1
FROM
(SELECT * FROM haltestellen_d WHERE "H_ID" <= 10000000) AS hd,
(SELECT * FROM haltestellen_d WHERE "H_ID" > 10000000) AS hv
WHERE hd."H_Name" = hv."H_Name") a
WHERE h."H_ID" =id1 ;

        """
        query = sql.format(haltestellen=haltestellen)
        self.run_query(query)

    def delete_multiple_abfahrten(self):
        """
        delete abfahrten marked with keep=False
        """
        self.logger.info(f'Removing duplicate departures')
        cur = self.conn.cursor()
        sql = """
-- lösche doppelte Fahrten
DELETE FROM departures WHERE keep = FALSE
OR keep IS NULL;
        """
        cur.execute(sql)
        self.logger.info(f'abfahrten deleted: {cur.statusmessage}')

        sql = """
-- remove copied dummy-haltestellen-ids
UPDATE trips f
SET stop_id = NULL WHERE stop_id > 10000000;

        """
        cur.execute(sql)
        self.logger.info(
            f'dummy-haltestellen_ids removed: {cur.statusmessage}')

    def reset_stop_id(self):
        """
        restet stop_id to None
        """
        sql = """
UPDATE trips SET stop_id = NULL;
"""
        self.run_query(sql)

    def set_eindeutige_stop_id(self):
        """
        Setze eindeutige stop_id
        """
        cur = self.conn.cursor()
        sql = """
-- setze für noch nicht gefundenen H_IDs Haltestellennummern aus verschiedenen Haltestellentabellen
UPDATE trips f
SET stop_id = h."H_ID"
FROM
(
SELECT
h."H_Name"
FROM stops h
GROUP BY h."H_Name"
HAVING count(*) = 1 ) h1,
stops h
WHERE h."H_Name" = h1."H_Name"
AND f.stop_id IS NULL
AND f."H_Name" = h."H_Name";

        """
        cur.execute(sql)
        self.logger.info(
            f'eindeutige stop_ids aus Haltestellen: {cur.statusmessage}')

    def update_stop_id_from_database(self):
        """
        Update stop_ids aus Deutchlandweiten Daten
        """
        self.logger.info(f'Updating stops from database')
        cur = self.conn.cursor()
        sql = """
-- setze für noch nicht gefundenen H_IDs Haltestellennummern aus verschiedenen Haltestellentabellen
UPDATE trips f
SET stop_id = h."H_ID"
FROM
stops h
WHERE f.stop_id IS NULL
AND f."H_Name" = h."H_Name";
        """

        sql2 = """
SELECT dblink_connect_u('conn', 'dbname=europe');
-- füge fehlende Haltestelle aus der Deutschland-Tabelle hinzu
INSERT INTO stops
("H_ID", "H_Name", geom, kreis)
SELECT "H_ID", "H_Name", st_transform(geom, {srid}) AS geom, kreis
FROM dblink('conn',
'SELECT h."H_ID", h."H_Name", h.geom, h.kreis
FROM timetables.haltestellen AS h') AS hd(
"H_ID" integer,
"H_Name" text,
geom geometry,
kreis text)
WHERE hd."H_ID" NOT IN (SELECT DISTINCT h."H_ID" FROM stops h)
--AND hd."H_ID" IN (SELECT DISTINCT f.stop_id FROM trips AS f)
AND hd."H_Name" IN (SELECT DISTINCT f."H_Name" FROM trips AS f);
"""

        query = sql
        self.logger.info(query)
        cur.execute(query)
        self.logger.info(f'{cur.statusmessage}')

        query = sql2.format(srid=self.target_srid)
        self.logger.info(query)
        cur.execute(query)
        self.logger.info(f'{cur.statusmessage}')

    def cleanup_haltestellen(self):
        """
        behalte nur die relevanten Haltestellen
        """
        self.logger.info(f'Cleaning up stops')
        cur = self.conn.cursor()
        sql = """
-- packe die Haltestellen in eine Backup-Tabelle

DROP TABLE IF EXISTS stops_backup;

SELECT *
INTO stops_backup
FROM stops;

TRUNCATE stops CASCADE;

UPDATE haltestellen h
SET kreis = hd.kreis
FROM haltestellen_d hd
WHERE hd."H_ID" = h."H_ID";


DELETE FROM stops h
LEFT JOIN trips f  on h."H_ID" = f.stop_id
LEFT JOIN departures a on h."H_ID" = a."H_ID"
WHERE
      f.stop_id IS NULL
      AND
      a."H_ID" IS NULL
;

"""
        self.run_query(sql)

    def update_stop_id_from_similar_routes(self):
        """
        """
        self.logger.info(f'Updating stops from similar routes')
        cur = self.conn.cursor()
        sql = """

--
UPDATE trips f
SET stop_id = c.stop1

FROM
(SELECT f.abfahrt_id, f.fahrt_index, b.stop1

FROM
(SELECT a."Fahrt_Name",a.stopname1, a.stopname0, a.stopname2, a.stop1
FROM
(
-- In jeder Zeile steht stop und stopname der Vorhaltestelle, aktuellen Haltestelle und Folgehaltestelle
SELECT
f."Fahrt_Name",
f.abfahrt_id,
f.fahrt_index,
f.stop_id AS stop1,
lag(f.stop_id) OVER(PARTITION BY f.abfahrt_id ORDER BY f.fahrt_index) AS stop0,
lead(f.stop_id) OVER(PARTITION BY f.abfahrt_id ORDER BY f.fahrt_index) AS stop2,
f."H_Name" AS stopname1,
lag(f."H_Name") OVER(PARTITION BY f.abfahrt_id ORDER BY f.fahrt_index) AS stopname0,
lead(f."H_Name") OVER(PARTITION BY f.abfahrt_id ORDER BY f.fahrt_index) AS stopname2
FROM trips f) AS a
-- Suche für jede Fahrt_Name alle eindeutigen Haltestellenfolgen (definiert durch den Haltestellennamen
GROUP BY a."Fahrt_Name",a.stopname1, a.stopname0, a.stopname2, a.stop1) b,

-- Fahrtentabelle mit den Haltestellennamen der Vorgänger und Nachfolgehaltestellen
(SELECT
f.abfahrt_id,
f.fahrt_index,
f."Fahrt_Name",
f.stop_id,
f."H_Name" AS stopname1,
lag(f."H_Name") OVER(PARTITION BY f.abfahrt_id ORDER BY f.fahrt_index) AS stopname0,
lead(f."H_Name") OVER(PARTITION BY f.abfahrt_id ORDER BY f.fahrt_index) AS stopname2
FROM
trips AS f ) AS f

-- Aktualisier unbekannte stop_id in der Fahrtentabelle aus den bekannten Haltestellenfolgen hinzu
WHERE f.stop_id IS NULL
AND f."Fahrt_Name" = b."Fahrt_Name"
AND f.stopname0 = b.stopname0
AND f.stopname2 = b.stopname2
AND f.stopname1 = b.stopname1
AND b.stop1 IS NOT NULL )c
WHERE f.abfahrt_id = c.abfahrt_id
AND f.fahrt_index = c.fahrt_index
;
        """
        cur.execute(sql)
        self.logger.info(
            f'UPDATE stop_id FROM similar routes: {cur.statusmessage}')

        sql = """

-- setze die Haltestellennummer bei Bussen auf die kleinste bekannte "H_ID" für den Haltestellennamen
UPDATE trips f
SET stop_id = a.stop_id
FROM (
SELECT f.abfahrt_id, f.fahrt_index,
min(h."H_ID") as stop_id
FROM trips f, stops h
WHERE f.stop_id IS NULL AND f."H_Name" = h."H_Name"
AND f."Fahrt_Name" LIKE 'Bus%'
GROUP BY f.abfahrt_id, f.fahrt_index )a
WHERE f.abfahrt_id = a.abfahrt_id
AND f.fahrt_index = a.fahrt_index;
"""

        cur.execute(sql)
        self.logger.info(
            f'UPDATE stop_id for busses: %s {cur.statusmessage}')

        sql = """
-- setze die Haltestellennummer bei Nicht-Bussen auf die größte bekannte "H_ID" für den Haltestellennamen
UPDATE trips f
SET stop_id = a.stop_id
FROM (
SELECT f.abfahrt_id, f.fahrt_index,
max(h."H_ID") as stop_id
FROM trips f, stops h
WHERE f.stop_id IS NULL AND f."H_Name" = h."H_Name"
AND f."Fahrt_Name" NOT LIKE 'Bus%'
GROUP BY f.abfahrt_id, f.fahrt_index )a
WHERE f.abfahrt_id = a.abfahrt_id
AND f.fahrt_index = a.fahrt_index;

"""
        cur.execute(sql)
        self.logger.info(f'UPDATE stop_id for busses: {cur.statusmessage}')

        sql = """
-- Setze die Koordinaten in haltestellen auf 0, wenn unbekannte Halteselle
-- Wenn keine "H_ID" übergeben wird, wird automatisch eine "H_ID" mit einem
-- serial Feld > 50000000 erzeugt
INSERT INTO stops ("H_Name", geom)
SELECT DISTINCT f."H_Name" AS "H_Name",
st_transform(ST_SETSRID(ST_MAKEPOINT(0,0), 4326), {srid}) AS geom
FROM trips f
WHERE f.stop_id IS NULL;

-- setze für Dummy-Haltestellen die stop_id
UPDATE trips f
SET stop_id = h."H_ID"
FROM
stops h
WHERE f.stop_id IS NULL
AND f."H_Name" = h."H_Name";
        """
        self.run_query(sql.format(srid=self.target_srid))

        sql = """
UPDATE trips SET stop_id_txt = stop_id::text;
        """
        self.run_query(sql)

    def fill_gtfs_stops(self):
        """
        """
        self.logger.info(f'Filling gtfs stops')

        # Stops
        sql = """

TRUNCATE gtfs_stops;

INSERT INTO gtfs_stops
SELECT
  h."H_ID" AS stop_id,
  h."H_Name" AS stop_name,
  st_y(st_transform(h.geom, 4326)) AS stop_lat,
  st_x(st_transform(h.geom, 4326)) AS stop_lon
FROM
  stops AS h
;

-- Generiere neuen langen Routennamen aus Fahrnamen -> Fahrtziel
UPDATE departures a
SET route_name_long =
regexp_replace(a."Fahrt_Name", ' +' ::text, ' ' ::text) || ' -> ' ::text ||
                 a."Fahrt_Ziel" ;

        """

        self.run_query(sql)

    def intersect_kreise(self, kreise='verwaltungsgrenzen.krs_2014_12'):
        """Intersect stops with kreisen"""
        sql = """
UPDATE stops h
SET kreis = k.rs
FROM {} k
WHERE st_intersects(k.geom, h.geom)
AND h.kreis IS NULL;
        """
        self.run_query(sql.format(kreise))

    def identify_kreis(self, kreise='verwaltungsgrenzen.krs_2014_12'):
        """
        Suche Kreis, in dem die meisten Haltestellen einer Linie liegen
        """
        self.logger.info(f'Identifying region')
        sql = """
-- Kreis, in der die meisten Abfahrten der Linie liegen, generieren
UPDATE departures a
SET kreis=b.kreis, kennz=k.kfz_kennzeichen
FROM
(
SELECT h.abfahrt_id, first_value(h.kreis) OVER(PARTITION BY h.abfahrt_id ORDER BY h.cnt DESC) AS kreis
FROM (
SELECT f.abfahrt_id, h.kreis, count(*) cnt
FROM trips f, stops h
WHERE h."H_ID" = f.stop_id
GROUP BY f.abfahrt_id, h.kreis) h
)b LEFT JOIN {kreise} k ON (k.rs = b.kreis)
WHERE a.abfahrt_id = b.abfahrt_id
;
"""
        self.run_query(sql.format(kreise=kreise))

    def mark_abfahrten_around_kreis(self, ags=None, kennz=None, bbox=None, ):
        """
        ist nocht nicht fertig
        """
        sql = """
-- Checke, ob Abfahrt im 5 km Radius den Kreis berührt.
UPDATE departures a
SET touches_kreis=TRUE
FROM
(
SELECT DISTINCT f.abfahrt_id
FROM trips f, stops h,
d.kr_2010 k
WHERE h."H_ID" = f.stop_id
AND st_dwithin(st_transform(h.geom, 31467), k.geom, 5000)
AND (k.kennz = 'SE' )) b
WHERE a.abfahrt_id = b.abfahrt_id
;

-- nur Routen im Kreis
UPDATE routes r
SET touches_kreis = TRUE
FROM(SELECT DISTINCT route_id FROM departures
WHERE touches_kreis
) b
WHERE r.route_id = b.route_id;
        """

        raise NotImplementedError('Funktion ist noch nicht fertig')

    def line_type(self):
        """
        """
        self.logger.info(f'Processing line types')
        sql = """

-- Linientyp suchen
UPDATE departures a
SET typ = regexp_replace(a."Fahrt_Name", ' *[0-9]+','');

UPDATE departures a
SET typ = substr(typ, 1, 3)
WHERE a.typ ~ '^Bus|AST|Fäh|NWB *';

-- route_short_name
UPDATE departures
SET route_short_name = regexp_replace("Fahrt_Name", '^'||typ||' *','');

-- agency_name
UPDATE departures
SET agency_name = kennz||' '||typ
WHERE typ = 'Bus' or typ = 'AST' or typ = 'ALT' or typ = 'Fäh'
or typ = 'U' or typ = 'Str' or typ = 'Schiff' or typ = 'S';
UPDATE departures
SET agency_name = typ
WHERE agency_name is NULL;

        """
        self.run_query(sql)

    def make_routes(self):
        """
        """
        self.logger.info(f'Creating routes')
        sql = """
TRUNCATE routes;
INSERT INTO routes (
route_id,
agency_name,
typ,
route_short_name,
abfahrten_list,
h_folge,
fz_folge,
hz_folge)
SELECT *
FROM
(
SELECT
row_number() OVER(ORDER BY agency_name, route_short_name)::integer AS route_id,
agency_name,
typ,
route_short_name,
abfahrten_list,
h_folge,
fz_folge,
hz_folge
FROM
(
SELECT
a.agency_name,
a.typ,
a.route_short_name,
array_accum(a.abfahrt_id) AS abfahrten_list,
h_folge,
fz_folge,
hz_folge
FROM
(SELECT
a.abfahrt_id,
a.agency_name,
a.typ, a.agency_name || '_' || a.route_short_name AS route_short_name,
ff.h_folge,
ff.fz_folge,
ff.hz_folge
FROM
(SELECT
f.abfahrt_id,
array_accum(stop_id) as h_folge,
array_accum("H_Ankunft" - lag_abfahrt) AS fz_folge,
array_accum("H_Abfahrt" - "H_Ankunft") AS hz_folge
FROM (
SELECT
abfahrt_id,stop_id,
"H_Ankunft",
"H_Abfahrt",
fahrt_index,
lag("H_Abfahrt") OVER(PARTITION BY abfahrt_id ORDER BY fahrt_index) AS lag_abfahrt
FROM
  trips f
ORDER BY abfahrt_id, fahrt_index)f
GROUP BY abfahrt_id
) ff, departures a
WHERE ff.abfahrt_id = a.abfahrt_id ) a
GROUP BY
a.agency_name, a.typ, a.route_short_name, h_folge, fz_folge, hz_folge
) r
) r;

-- Routen den abfahrten zuweisen
UPDATE departures a
SET route_id = r.route_id
FROM routes r
WHERE a.abfahrt_id = ANY(r.abfahrten_list)
;
        """
        self.run_query(sql)

    def make_agencies(self):
        """
        """
        self.logger.info(f'Creating agencies')
        sql = """
-- agency-Tabelle erzeugen
TRUNCATE agencies;
INSERT INTO agencies (agency_id, agency_name)
SELECT
row_number() OVER(ORDER BY typ, agency_name)::integer AS agency_id,
agency_name
FROM (SELECT DISTINCT typ, agency_name FROM routes) r;

TRUNCATE gtfs_agency;
INSERT INTO gtfs_agency (agency_id, agency_name)
SELECT agency_id, agency_name
FROM agencies;


-- Langen Liniennamen generieren
UPDATE routes r
SET route_long_name = r.agency_name || '_' || r.route_short_name || ' -> ' || h."H_Name"
FROM stops h
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
        self.logger.info(f'Creating shapes')
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
LEFT JOIN route_types t ON (r.typ = t.typ),
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
st_x(st_transform(h.geom, 4326)) AS shape_pt_lon,
st_y(st_transform(h.geom, 4326)) AS shape_pt_lat
FROM
(SELECT
shape_id,
unnest(h_folge) AS h_id,
generate_subscripts(h_folge, 1) AS shape_pt_sequence
FROM shapes s) r3,
stops h
WHERE r3.h_id = h."H_ID"
ORDER BY shape_id, shape_pt_sequence;
-- delete invalid points
DELETE FROM gtfs_shapes
WHERE shape_pt_lon = 0 AND shape_pt_lat = 0;

UPDATE routes r
SET geom = st_setsrid(st_makeline(points), 4326)
FROM (
SELECT shape_id, array_agg(point ORDER BY s.shape_id, s.shape_pt_sequence) AS points
FROM (SELECT s.shape_id, s.shape_pt_sequence, st_makepoint(s.shape_pt_lon, s.shape_pt_lat) AS point
FROM gtfs_shapes s
) s
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
FROM routes r, departures a
WHERE a.route_id = r.route_id
--AND r.touches_kreis
;
        """
        self.run_query(sql)

    def make_stop_times(self):
        """
        """
        self.logger.info(f'Creating stop times')
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

FROM trips f, departures a
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
        """.format(today=self.today.day)
        self.run_query(sql)

    def export_gtfs(self):
        """
        exports the data to the path
        """
        path = os.path.join(self.base_path,
                            self.destination_db,
                            self.subfolder)

        with Connection(self.login) as conn:
            self.conn = conn
            self.set_search_path()
            cur = self.conn.cursor()
            sql = '''SET CLIENT_ENCODING TO '{encoding}';'''
            encoding = 'UTF8'
            cur.execute(sql.format(encoding=encoding))
            tables = ['stops', 'agency', 'stop_times', 'routes', 'trips', 'shapes',
                      'calendar', 'calendar_dates', 'transfers']
            folder = path.replace('~', os.environ['HOME'])
            self.make_folder(folder)
            zipfilename = os.path.join(folder,
                                       '{}.zip'.format(self.destination_db))
            with zipfile.ZipFile(zipfilename, 'w') as z:
                for table in tables:
                    self.logger.info(f'Exporting table {table} to gtfs')
                    tn = 'gtfs_{tn}'.format(tn=table)
                    tablename = '{tn}.txt'.format(tn=table)
                    fn = os.path.join(folder, tablename)
                    self.logger.info('write {}'.format(fn))
                    with open(fn, 'w') as f:
                        sql = self.conn.copy_sql.format(tn=tn, fn=fn)
                        self.logger.info(sql)
                        cur.copy_expert(sql, f)
                    z.write(fn, tablename)
                    os.remove(fn)

            sql = '''RESET CLIENT_ENCODING;'''
            cur.execute(sql)


if __name__ == '__main__':
    parser = ArgumentParser(
        description="Scrape Stops in a given bounding box")

    parser.add_argument('--host', action="store",
                        help="host",
                        dest="host", default='localhost')
    parser.add_argument("-p", '--port', action="store",
                        help="port", type=int,
                        dest="port", default=5432)

    parser.add_argument("-U", '--user', action="store",
                        help="database user",
                        dest="user", default='osm')
    parser.add_argument('-n', '--destination-db', action="store",
                        help="destination database",
                        dest="destination_db", default='extract')
    parser.add_argument('--source-db', action="store",
                        help="source database with stop coordinates (Default: Europe)",
                        dest="source_db", default='europe')

    parser.add_argument('--date', action="store", type=str,
                        help="date in Format DD.MM.YYYY",
                        dest="date")

    parser.add_argument('--only-one-day', action='store_true',
                        help='''
if only-one-day is selected, the gtfs-feed will be valid only on the selected date,
otherwise its valid all days''', dest='only_one_day', default=False)

    parser.add_argument('--subfolder', action="store",
                        help="""subfolder within the project folder
                        to store the gtfs files""",
                        dest="subfolder", default='otp')

    parser.add_argument('--base-path', action="store", type=str,
                        help="folder to store the resulting gtfs files",
                        dest="base_path", default=r'~/gis/projekte')

    parser.add_argument('--tbl-kreise', action="store", type=str,
                        help="table with the county geometries",
                        dest="tbl_kreise",
                        default='verwaltungsgrenzen.krs_2014_12')

    options = parser.parse_args()

    hafas = HafasDB2GTFS(db=options.destination_db,
                         date=options.date,
                         only_one_day=options.only_one_day,
                         base_path=options.base_path,
                         subfolder=options.subfolder,
                         tbl_kreise=options.tbl_kreise)
    hafas.set_login(host=options.host, port=options.port, user=options.user)
    hafas.get_target_boundary_from_dest_db()

    hafas.convert()
    hafas.export_gtfs()
