#!/usr/bin/env python
#coding:utf-8
from argparse import ArgumentParser
import numpy as np
import logging
logger = logging.getLogger()
logger.addHandler(logging.StreamHandler())
logger.level = logging.DEBUG
import os
import datetime
from extractiontools.connection import Connection, DBApp
from extractiontools.utils.get_date import Date

class CleanHafas(DBApp):
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
            self.create_aggregate_functions()
            self.count()
            self.delete_invalid_fahrten()
            self.set_stop_id()
            self.write_calendar()
            self.workaround_sommerzeit()
            self.test_for_negative_travel_times()
            self.set_arrival_to_day_before()
            self.set_departure_to_day_before()
            self.shift_trips()
            self.test_for_negative_travel_times()
            self.set_ein_aus()

            self.count_multiple_abfahrten()
            self.show_multiple_trips()
            self.save_haltestellen_id()
            self.delete_multiple_abfahrten()

            # Haltestellen
            self.update_haltestellen_d()

            self.reset_stop_id()
            self.set_eindeutige_stop_id()
            self.update_stop_id_from_database()

            self.cleanup_haltestellen()

            self.update_stop_id_from_similar_routes()

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

    def create_aggregate_functions(self):
        """
        """
        sql = """
DROP AGGREGATE IF EXISTS array_accum (anyelement);
CREATE AGGREGATE array_accum (anyelement)
(
    sfunc = array_append,
    stype = anyarray,
    initcond = '{}'
);
        """
        self.run_query(sql)

    def count(self):
        sql = 'SELECT count(*) FROM haltestellen;'
        cur = self.conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        logger.info('Haltestellen: %s, ' % rows[0][0])

        sql = 'SELECT count(*) FROM abfahrten;'
        cur = self.conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        logger.info('Abfahrten: %s ' % rows[0][0])

        sql = 'SELECT count(*) FROM fahrten;'
        cur = self.conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        logger.info('Fahrten: %s' % rows[0][0])

    def delete_invalid_fahrten(self):
        sql = """
DELETE FROM fahrten WHERE abfahrt_id IN (
SELECT f.abfahrt_id FROM fahrten f LEFT JOIN abfahrten a
ON f.abfahrt_id = a.abfahrt_id
WHERE a.abfahrt_id IS NULL) ;
        """
        cur = self.conn.cursor()
        cur.execute(sql)
        logger.info('Delete invalid trips: %s' % cur.statusmessage)

    def set_stop_id(self):
        sql = """
UPDATE fahrten SET stop_id = "H_ID" WHERE "H_ID" IS NOT NULL;
UPDATE fahrten SET stop_id = NULL WHERE stop_id >= 10000000;
DELETE FROM haltestellen WHERE "H_ID" >= 10000000;
        """
        cur = self.conn.cursor()
        cur.execute(sql)
        logger.info('Set Stop ID: %s' % cur.statusmessage)

    def workaround_sommerzeit(self):
        """
        add 1 hour
        """
        sql = '''
UPDATE abfahrten SET
"Fahrt_Abfahrt" = "Fahrt_Abfahrt" + '1 hour'::INTERVAL;
UPDATE fahrten SET
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
INSERT INTO gtfs_calendar VALUES(1,0,0,0,0,0,0,0,{today},{tomorrow});
TRUNCATE gtfs_calendar_dates;
INSERT INTO gtfs_calendar_dates VALUES(1,{today},1);
INSERT INTO gtfs_calendar_dates VALUES(1,{tomorrow},1);
        '''.format(today=str_today, tomorrow=str_tomorrow)
        self.run_query(sql)

    def set_arrival_to_day_before(self):
        sql = """

-- setze Ankunftstag für Ankunftszeiten zwischen 12:00 und 23:59 auf Vortag
UPDATE fahrten f
SET
"H_Ankunft" = "H_Ankunft" - interval '1 day'
FROM
--SELECT * FROM fahrten f,
(SELECT abf.abfahrt_id, abf.d0, abf.h0, ank.d1, ank.h1, a."Fahrt_URL"
FROM
(SELECT abfahrt_id, h1, d1 FROM
(select abfahrt_id,
extract(hour from "H_Ankunft") AS h1 ,
extract(day from "H_Ankunft") AS d1,
 "H_Ankunft", fahrt_index,
 max(fahrt_index) over (PARTITION BY abfahrt_id) AS maxindex
FROM fahrten) f1
WHERE fahrt_index = maxindex) ank,
(SELECT
abfahrt_id,
extract(hour from "H_Abfahrt") AS h0,
extract(day from "H_Abfahrt") AS d0
FROM fahrten
 WHERE fahrt_index = 1
 ) AS abf,
abfahrten a
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
        logger.info('set_arrival_to_day_before: %s' % cur.statusmessage)

    def set_departure_to_day_before(self):
        """

        """
        sql = """
--setze Abfahrtstag für Ankunftszeiten zwischen 12:00 und 23:59 auf Folgetag
UPDATE fahrten f
SET
"H_Abfahrt" = "H_Abfahrt" - interval '1 day'
FROM
--SELECT * FROM fahrten f,
(SELECT abf.abfahrt_id, abf.d0, abf.h0, ank.d1, ank.h1, a."Fahrt_URL"
FROM
(SELECT abfahrt_id, h1, d1
FROM (select abfahrt_id,
extract(hour from "H_Ankunft") AS h1 ,
extract(day from "H_Ankunft") AS d1, "H_Ankunft",
fahrt_index, max(fahrt_index) over (PARTITION BY abfahrt_id) AS maxindex
FROM fahrten
) f1
WHERE fahrt_index = maxindex) ank,
(SELECT
abfahrt_id,
extract(hour from "H_Abfahrt") AS h0,
extract(day from "H_Abfahrt") AS d0 FROM fahrten WHERE fahrt_index = 1
) AS abf,
abfahrten a
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
        logger.info('set_departure_to_day_before: %s' % cur.statusmessage)

    def shift_trips(self):
        sql1 = """
-- korrigiere Fahrten über Tagesgrenzen hinaus
--verschiebe Fahrten nach vorne, so dass nur Fahrten heute und morgen auftreten
UPDATE fahrten f
SET "H_Abfahrt" = "H_Abfahrt" + '1 day'::INTERVAL,
"H_Ankunft" = "H_Ankunft" + '1 day'::INTERVAL
FROM (
SELECT DISTINCT abfahrt_id
FROM fahrten
WHERE date_part('day', "H_Abfahrt") < {today}) v
WHERE f.abfahrt_id = v.abfahrt_id
;
""".format(today=self.day)

        sql2 = """
--verschiebe Fahrten nach hinten, so dass nur Fahrten heute und morgen auftreten
UPDATE fahrten f
SET "H_Abfahrt" = "H_Abfahrt" - '1 day'::INTERVAL,
"H_Ankunft" = "H_Ankunft" - '1 day'::INTERVAL
FROM (
SELECT DISTINCT abfahrt_id
FROM fahrten
WHERE date_part('day', "H_Ankunft") > {tomorrow}) v
WHERE f.abfahrt_id = v.abfahrt_id
;

""".format(tomorrow = self.day + 1)

        cur = self.conn.cursor()
        n_updated = 1
        while n_updated:
            cur.execute(sql1)
            msg = cur.statusmessage
            n_updated = int(msg.split(' ')[1])
            logger.info('Shift Trips forward: %s' % cur.statusmessage)

        n_updated = 1
        while n_updated:
            cur.execute(sql2)
            msg = cur.statusmessage
            n_updated = int(msg.split(' ')[1])
            logger.info('Shift Trips backward: %s' % cur.statusmessage)

    def reset_ein_aus(self):
        sql1 = """
UPDATE fahrten
SET "H_Abfahrt" = NULL
WHERE ein = FALSE;

"""
        sql2 = """
UPDATE fahrten
SET "H_Ankunft" = NULL
WHERE aus = FALSE;

"""
        cur = self.conn.cursor()
        cur.execute(sql1)
        logger.info('no Entry: %s' % cur.statusmessage)
        cur.execute(sql2)
        logger.info('no Exit: %s' % cur.statusmessage)

    def set_ein_aus(self):
        sql1 = """
UPDATE fahrten
SET ein = FALSE
WHERE "H_Abfahrt" IS NULL
"""
        sql2 = """
UPDATE fahrten
SET aus = FALSE
WHERE "H_Ankunft" IS NULL
"""
        cur = self.conn.cursor()
        cur.execute(sql1)
        logger.info('no Entry: %s' % cur.statusmessage)
        cur.execute(sql2)
        logger.info('no Exit: %s' % cur.statusmessage)

        sql3 = """
UPDATE fahrten f
SET "H_Abfahrt" = f."H_Ankunft"
FROM
(SELECT abfahrt_id, fahrt_index FROM
(select abfahrt_id,
 "H_Ankunft", "H_Abfahrt", fahrt_index,
 max(fahrt_index) over (PARTITION BY abfahrt_id) AS maxindex
FROM fahrten) f1
WHERE fahrt_index < maxindex
AND "H_Ankunft" IS NOT NULL AND "H_Abfahrt" IS NULL) ank
WHERE ank.abfahrt_id = f.abfahrt_id
AND ank.fahrt_index = f.fahrt_index;
        """
        cur.execute(sql3)
        logger.info('Missing Departure Time added : %s' % cur.statusmessage)

        sql4 = """
UPDATE fahrten f
SET "H_Ankunft" = "H_Abfahrt"
WHERE fahrt_index > 1
AND "H_Ankunft" IS NULL AND "H_Abfahrt" IS NOT NULL;
        """
        cur.execute(sql4)
        logger.info('Missing Arrival Time added : %s' % cur.statusmessage)


    def test_for_negative_travel_times(self):
        cur = self.conn.cursor()
        sql = """
SELECT count(*) FROM (
SELECT *,  lag("H_Abfahrt") OVER(PARTITION BY abfahrt_id ORDER BY fahrt_index) AS ab0 FROM fahrten f )f
where "H_Ankunft" < ab0;

        """
        cur.execute(sql)
        result = cur.fetchone()

        sql = """
SELECT count(*) FROM fahrten f
where "H_Abfahrt" < "H_Ankunft";

        """
        cur.execute(sql)
        result2 = cur.fetchone()
        if result[0] or result2[0]:
            logger.warn('''There exist still negative travel times with
            departures before arrivals!''')
            if result[0]:
                sql = """
        SELECT abfahrt_id FROM (
        SELECT *,  lag("H_Abfahrt") OVER(PARTITION BY abfahrt_id ORDER BY fahrt_index) AS ab0 FROM fahrten f )f
        where "H_Ankunft" < ab0;

                """
                cur.execute(sql)
                abfahrten = np.array(cur.fetchall())
                logger.warning('%s' % abfahrten)

            if result2[0]:
                sql = """
SELECT abfahrt_id FROM fahrten f
where "H_Abfahrt" < "H_Ankunft";

                """
                cur.execute(sql)
                abfahrten = np.array(cur.fetchall())
                logger.warning('%s' % abfahrten)
            #raise ValueError('''There exist still negative travel times with
            #departures before arrivals!''')
        else:
            logger.info('no negative travel times')

    def count_multiple_abfahrten(self):
        """
        count the number of occurences of abfahrten
        and save into abfahrt_id_final
        """

        cur = self.conn.cursor()
        sql = """
-- zähle die doppelten Fahrten durch
UPDATE abfahrten a
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
              FROM abfahrten a,
                   (
                     SELECT f.abfahrt_id,
                            f."H_ID",
                            f."H_Ankunft",
                            f."H_Name",
                            f.fahrt_index,
                            row_number() OVER(PARTITION BY f.abfahrt_id
                     ORDER BY f.fahrt_index DESC) rn
                     FROM fahrten f
                   ) f1, -- letzte Haltestelle der Fahrt row_number() mit ORDER BY fahrt_index descending, also rückwärte

                   fahrten f, -- erste Haltestelle der Fahrt
                   fahrten f2 -- zweite Haltestelle der Fahrt
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
        logger.info('Multiple_abfahrten: %s' % cur.statusmessage)

        sql = """
        -- zähle, wie viele doppelte vorhanden
        SELECT keep,
               count(*)
        FROM abfahrten
        GROUP BY keep;
        """
        cur.execute(sql)
        rows = cur.fetchall()
        msg = 'keep abfahrten: {0.keep}: {0.count}'
        for row in rows:
            logger.info(msg.format(row))

    def show_multiple_trips(self):
        cur = self.conn.cursor()
        sql = '''
SELECT f.*, a.keep FROM fahrten f, (SELECT abfahrt_id, abfahrt_id_final FROM abfahrten WHERE keep = False) a1, abfahrten a
WHERE f.abfahrt_id = a.abfahrt_id AND a1.abfahrt_id_final = a.abfahrt_id_final
ORDER BY f."Fahrt_Name", f.abfahrt_id, f.fahrt_index;
'''
        cur.execute(sql)
        rows = cur.fetchall()
        msg = '{0.Fahrt_Name}\t{0.abfahrt_id}\t{0.fahrt_index}\t{0.H_Ankunft} - {0.H_Abfahrt}\t{0.H_ID}\t{0.H_Name}'
        for row in rows:
            m = msg.format(row)
            logger.info(m)


    def save_haltestellen_id(self):
        """
        save H_ID into stop_id before deleting a trip
        """
        cur = self.conn.cursor()
        sql = """
-- sichere vor dem löschen H_ID, die bei doppelten Fahrten gefunden wurden in Spalte stop_id
UPDATE fahrten f
SET stop_id = f2."H_ID"
FROM
     (
       SELECT a.abfahrt_id_final,
              f.fahrt_index,
              max(f."H_ID") AS "H_ID"
       FROM fahrten f,
            abfahrten a
       WHERE f.abfahrt_id = a.abfahrt_id
       GROUP BY a.abfahrt_id_final,
                f.fahrt_index
     ) f2
WHERE f.abfahrt_id = f2.abfahrt_id_final
      AND f.fahrt_index = f2.fahrt_index
      AND f2."H_ID" IS NOT NULL;
        """
        cur.execute(sql)
        logger.info('save haltestellen_ids: %s' % cur.statusmessage)

    def update_haltestellen_d(self,
                              haltestellen='haltestellen_mitteleuropa.haltestellen'):
        """
        Aktualisiere Haltestellen_d
        """
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
        cur = self.conn.cursor()
        sql = """
-- lösche doppelte Fahrten
DELETE FROM abfahrten WHERE keep = FALSE
OR keep IS NULL;
        """
        cur.execute(sql)
        logger.info('abfahrten deleted: %s' % cur.statusmessage)

        sql = """
-- remove copied dummy-haltestellen-ids
UPDATE fahrten f
SET stop_id = NULL WHERE stop_id > 10000000;

        """
        cur.execute(sql)
        logger.info('dummy-haltestellen_ids removed: %s' % cur.statusmessage)

    def reset_stop_id(self):
        """
        restet stop_id to None
        """
        sql = """
UPDATE fahrten SET stop_id = NULL;
"""
        self.run_query(sql)

    def set_eindeutige_stop_id(self):
        """
        Setze eindeutige stop_id
        """
        cur = self.conn.cursor()
        sql = """
-- setze für noch nicht gefundenen H_IDs Haltestellennummern aus verschiedenen Haltestellentabellen
UPDATE fahrten f
SET stop_id = h."H_ID"
FROM
(
SELECT
h."H_Name"
FROM haltestellen h
GROUP BY h."H_Name"
HAVING count(*) = 1 ) h1,
haltestellen h
WHERE h."H_Name" = h1."H_Name"
AND f.stop_id IS NULL
AND f."H_Name" = h."H_Name";

        """
        cur.execute(sql)
        logger.info('eindeutige stop_ids aus Haltestellen: %s' % cur.statusmessage)

    def update_stop_id_from_database(self):
        """
        Update stop_ids aus Deutchlandweiten Daten
        """
        cur = self.conn.cursor()
        sql = """
-- setze für noch nicht gefundenen H_IDs Haltestellennummern aus verschiedenen Haltestellentabellen
UPDATE fahrten f
SET stop_id = h."H_ID"
FROM
{haltestellen} h
WHERE f.stop_id IS NULL
AND f."H_Name" = h."H_Name";

        """

        sql2 = """
-- füge fehlende Haltestelle aus der Deutschland-Tabelle hinzu
INSERT INTO haltestellen
("H_ID", "H_Name", geom, kreis)
SELECT "H_ID", "H_Name", geom, kreis
FROM {haltestellen} hd
WHERE hd."H_ID" NOT IN (SELECT DISTINCT h."H_ID" FROM haltestellen h)
AND hd."H_ID" IN (SELECT DISTINCT f.stop_id FROM fahrten AS f);
"""

        h_tables = ['haltestellen_d']

        for h_table in h_tables:
            query = sql.format(haltestellen=h_table)
            logger.info(query)
            cur.execute(query)
            logger.info('{h}: {msg}'.format(h=h_table, msg=cur.statusmessage))

            query = sql2.format(haltestellen=h_table)
            logger.info(query)
            cur.execute(query)
            logger.info('{h}: {msg}'.format(h=h_table, msg=cur.statusmessage))




    def cleanup_haltestellen(self):
        """
        behalte nur die relevanten Haltestellen
        """
        cur = self.conn.cursor()
        sql = """
-- packe die Haltestellen in eine Backup-Tabelle

DROP TABLE IF EXISTS haltestellen_backup;

SELECT *
INTO haltestellen_backup
FROM haltestellen;

TRUNCATE haltestellen CASCADE;

UPDATE haltestellen h
SET kreis = hd.kreis
FROM haltestellen_d hd
WHERE hd."H_ID" = h."H_ID";


INSERT INTO haltestellen
SELECT hb.* FROM haltestellen_backup hb,
(
SELECT DISTINCT hb."H_ID" FROM haltestellen_backup hb
LEFT JOIN fahrten f  on hb."H_ID" = f.stop_id
LEFT JOIN abfahrten a on hb."H_ID" = a."H_ID"
WHERE
      f.stop_id IS NOT NULL
      OR
      a."H_ID" IS NOT NULL
) h2
WHERE hb."H_ID" = h2."H_ID"
;

"""
        self.run_query(sql)


    def update_stop_id_from_similar_routes(self):
        """
        """
        cur = self.conn.cursor()
        sql = """

--
UPDATE fahrten f
SET stop_id = c.stop1
FROM
(SELECT f.abfahrt_id, f.fahrt_index, b.stop1

FROM
(SELECT a."Fahrt_Name",a.stopname1, a.stopname0, a.stopname2, a.stop1
FROM
(
-- In jeder Zeile steht stop und stopname der Vorhaltestelle, aktuellen Haltestelle und Folgehaltestelle
SELECT f."Fahrt_Name", f.abfahrt_id, f.fahrt_index,
f.stop_id AS stop1,
lag(f.stop_id) OVER(PARTITION BY f.abfahrt_id ORDER BY f.fahrt_index) AS stop0,
lead(f.stop_id) OVER(PARTITION BY f.abfahrt_id ORDER BY f.fahrt_index) AS stop2,
f."H_Name" AS stopname1,
lag(f."H_Name") OVER(PARTITION BY f.abfahrt_id ORDER BY f.fahrt_index) AS stopname0,
lead(f."H_Name") OVER(PARTITION BY f.abfahrt_id ORDER BY f.fahrt_index) AS stopname2
FROM fahrten f) AS a
-- Suche für jede Fahrt_Name alle eindeutigen Haltestellenfolgen (definiert durch den Haltestellennamen
GROUP BY a."Fahrt_Name",a.stopname1, a.stopname0, a.stopname2, a.stop1) b,

-- Fahrtentabelle mit den Haltestellennamen der Vorgänger und Nachfolgehaltestellen
(SELECT f.abfahrt_id, f.fahrt_index, f."Fahrt_Name", f.stop_id, f."H_Name" AS stopname1,
lag(f."H_Name") OVER(PARTITION BY f.abfahrt_id ORDER BY f.fahrt_index) AS stopname0,
lead(f."H_Name") OVER(PARTITION BY f.abfahrt_id ORDER BY f.fahrt_index) AS stopname2
FROM
fahrten AS f ) AS f

-- Aktualisier unbekannte stop_id in der Fahrtentabelle aus den bekannten Haltestellenfolgen hinzu
WHERE f.stop_id IS NULL
AND f."Fahrt_Name" = b."Fahrt_Name"
AND f.stopname0 = b.stopname0
AND f.stopname2 = b.stopname2
AND f.stopname1 = b.stopname1
AND b.stop1 IS NOT NULL )c
WHERE f.abfahrt_id = c.abfahrt_id AND f.fahrt_index = c.fahrt_index
;
        """
        cur.execute(sql)
        logger.info('UPDATE stop_id FROM similar routes: %s' % cur.statusmessage)

        sql = """

-- setze die Haltestellennummer bei Bussen auf die kleinste bekannte "H_ID" für den Haltestellennamen
UPDATE fahrten f
SET stop_id = a.stop_id
FROM (
SELECT f.abfahrt_id, f.fahrt_index,
min(h."H_ID") as stop_id FROM fahrten f, haltestellen h
WHERE f.stop_id IS NULL AND f."H_Name" = h."H_Name"
AND f."Fahrt_Name" LIKE 'Bus%'
GROUP BY f.abfahrt_id, f.fahrt_index )a
WHERE f.abfahrt_id = a.abfahrt_id
AND f.fahrt_index = a.fahrt_index;
"""

        cur.execute(sql)
        logger.info('UPDATE stop_id for busses: %s' % cur.statusmessage)

        sql = """
-- setze die Haltestellennummer bei Nicht-Bussen auf die größte bekannte "H_ID" für den Haltestellennamen
UPDATE fahrten f
SET stop_id = a.stop_id
FROM (
SELECT f.abfahrt_id, f.fahrt_index,
max(h."H_ID") as stop_id FROM fahrten f, haltestellen h
WHERE f.stop_id IS NULL AND f."H_Name" = h."H_Name"
AND f."Fahrt_Name" NOT LIKE 'Bus%'
GROUP BY f.abfahrt_id, f.fahrt_index )a
WHERE f.abfahrt_id = a.abfahrt_id
AND f.fahrt_index = a.fahrt_index;

"""
        cur.execute(sql)
        logger.info('UPDATE stop_id for busses: %s' % cur.statusmessage)

        sql = """
-- Setze die Koordinaten in haltestellen auf 0, wenn unbekannte Halteselle
-- Wenn keine "H_ID" übergeben wird, wird automatisch eine "H_ID" mit einem
-- serial Feld > 50000000 erzeugt
INSERT INTO haltestellen ("H_Name", geom)
SELECT DISTINCT f."H_Name" AS "H_Name",
ST_SETSRID(ST_MAKEPOINT(0,0), 4326) AS geom
FROM fahrten f
WHERE f.stop_id IS NULL;

-- setze für Dummy-Haltestellen die stop_id
UPDATE fahrten f
SET stop_id = h."H_ID"
FROM
haltestellen h
WHERE f.stop_id IS NULL
AND f."H_Name" = h."H_Name";
        """
        self.run_query(sql)

        sql = """
UPDATE fahrten SET stop_id_txt = stop_id::text;
        """
        self.run_query(sql)


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
                        dest="user", default='max')

    parser.add_argument('--day', action="store", type=int,
                        help="day, default: day of today",
                        dest="day")
    parser.add_argument('--month', action="store", type=int,
                        help="month, default: month of today",
                        dest="month")
    parser.add_argument('--year', action="store", type=int,
                        help="year, default: year of today",
                        dest="year")
    parser.add_argument('--source-db', action="store",
                        help="source database",
                        dest="source_db", default='dplus')

    options = parser.parse_args()
    date = Date(options.year, options.month, options.day)

    hafas = CleanHafas(date=options.date)
    hafas.set_login(host=options.host, port=options.port, user=options.user)
    hafas.convert()
    #hafas.export_gtfs(path=r'W:\\mobil\66 Zwischenablage Ben\GTFS_Schleswig_Flensburg\GTFS_neu')
