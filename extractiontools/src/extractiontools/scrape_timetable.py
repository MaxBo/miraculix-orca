#!/usr/bin/env python
# coding:utf-8

from argparse import ArgumentParser

import time
import datetime
from extractiontools.scrape_stops import ScrapeStops, Connection
from extractiontools.utils.bahn_query import BahnQuery


class ScrapeTimetable(ScrapeStops):

    sql_date_format = '%Y-%m-%d'
    sql_time_format = '%H:%M'
    sql_timestamp_format = f'{sql_date_format} {sql_time_format}'
    max_retries = 10

    def __init__(self,
                 destination_db: str,
                 date: datetime.datetime,
                 source_db: str,
                 recreate_tables: bool = True,
                 logger=None):
        """"""
        super().__init__(destination_db=destination_db, source_db=source_db,
                         logger=logger)
        self.date = date
        self.recreate_tables = recreate_tables

    def scrape(self):
        """scrape timetables"""
        with Connection(login=self.login) as conn:
            self.conn = conn
            self.create_timetable_tables()
            if self.recreate_tables:
                self.truncate_timetables()
            self.conn.commit()

            self.get_fahrten_for_stops()
            self.conn.commit()
            self.add_missing_stops()
            self.conn.commit()

    def create_timetable_tables(self):
        """(Re-Create the timetable tables)"""
        self.logger.info(f'(Re)creating timetables and indexes')
        sql = f"""
        CREATE TABLE IF NOT EXISTS "{self.schema}".abfahrten
        (
          "Fahrt_URL" text,
          "Fahrt_Name" text,
          "Fahrt_Abfahrt" timestamp(0) with time zone,
          "suchdatum" timestamp(0) with time zone,
          abfahrt_id BIGSERIAL PRIMARY KEY,
          "H_ID" integer,
          "Fahrt_Start" text,
          "Fahrt_Ziel" text,
          "Fahrt_Nr" text,
          CONSTRAINT abfahrten_fk FOREIGN KEY ("H_ID")
              REFERENCES "{self.schema}".haltestellen ("H_ID") MATCH SIMPLE
              ON UPDATE NO ACTION ON DELETE NO ACTION
        );

        CREATE INDEX IF NOT EXISTS abfahrten_sh_idx
          ON "{self.schema}".abfahrten
          USING btree
          ("Fahrt_Name", "Fahrt_Ziel");

        CREATE TABLE IF NOT EXISTS "{self.schema}".fahrten
        (
          "Fahrt_Name" text,
          "H_Ankunft" timestamp(0) with time zone,
          "H_Abfahrt" timestamp(0) with time zone,
          "H_Name" text,
          abfahrt_id bigint NOT NULL,
          fahrt_index integer NOT NULL,
          "H_ID" integer,
          CONSTRAINT fahrten_idx PRIMARY KEY (abfahrt_id, fahrt_index),
          CONSTRAINT fahrten_fk FOREIGN KEY (abfahrt_id)
              REFERENCES "{self.schema}".abfahrten (abfahrt_id) MATCH SIMPLE
              ON UPDATE CASCADE ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS fahrten_idx1
          ON "{self.schema}".fahrten
          USING btree
          ("H_Name", "H_Abfahrt");
        """
        cursor = self.conn.cursor()
        cursor.execute(sql)

    def truncate_timetables(self):
        """Truncate the timetables"""
        sql = f"""
        TRUNCATE "{self.schema}".abfahrten CASCADE;
        TRUNCATE "{self.schema}".fahrten;
        """
        cursor = self.conn.cursor()
        cursor.execute(sql)

    def get_fahrten_for_stops(self):
        """get the stops in the area"""
        sql = f"""
        SELECT "H_ID", "H_Name"
        FROM "{self.schema}".haltestellen
        WHERE in_area;
        """
        cursor = self.conn.cursor()
        cursor.execute(sql)
        rows = cursor.fetchall()
        for r, row in enumerate(rows):
            self.logger.info(
                f'Looking for Routes at stop "{row[1]}"... '
                f'({r+1}/{len(rows)})')
            stop_id = row[0]

            db_query = BahnQuery(dt=self.date, timeout=0)
            retries = 0
            journeys = []
            while retries < self.max_retries:
                try:
                    journeys = db_query.scrape_journeys(stop_id)
                    break
                except ConnectionError as e:
                    self.logger.warning(f'{str(e)} Retrying...')
                time.sleep(15)
                retries += 1
            i = al_i = 0
            for j, journey in enumerate(journeys):
                try:
                    already_in = self.check_journey(journey, stop_id)
                    if already_in:
                        #self.logger.info(f"Route \"{journey['name']}\" to "
                                         #f"{journey['destination']} already in "
                                         #"database. Skipping... "
                                         #f"({j+1}/{len(journeys)})")
                        al_i += 1
                        continue
                    route = db_query.scrape_route(journey['url'])
                    self.add_journey(journey, route, stop_id)
                    #self.logger.info(
                        #f"Route {route[0]['departure'].strftime('%H:%M')} "
                        #f"\"{journey['name']}\" {journey['number']} "
                        #f"{route[0]['station_name']} -> {journey['destination']} added "
                        #f"({j+1}/{len(journeys)})")
                # ToDo: sometimes there is an IndexError in the journey, did
                # not debug it yet
                except IndexError as e:
                    msg = str(e)
                    self.logger.error(f"{msg} - There might be a change in the HTML structure of the DB-Reiseauskunft")
                i += 1
            self.logger.info(
                f"{len(journeys)} Routes processed. {al_i} Routes were skipped (already in database)")
            if i == 0:
                continue
            self.conn.commit()
            self.logger.info(
                f'Inserted {i} Route(s) for "{row[1]}".')

    def clear_journeys(self, stop_id):
        sql = f'''
        DELETE FROM "{self.schema}".abfahrten
        WHERE "H_ID"=%(stop_id)s
        AND suchdatum=%(date)s;
        '''
        self.run_query(sql, vars={'stop_id': stop_id, 'date': self.date, })

    def add_journey(self, journey, route, stop_id):
        # dt_txt = journey['departure'].strftime('%H:%M')
        dt_txt = route[0]['departure'].strftime(self.sql_timestamp_format)
        sql = f'''
        INSERT INTO "{self.schema}".abfahrten
        ("Fahrt_URL", "Fahrt_Name",
        "Fahrt_Abfahrt", "Fahrt_Start", "Fahrt_Ziel",
        "Fahrt_Nr", "H_ID",
        suchdatum)
        VALUES
          (%(url)s, %(journey_name)s, %(dt_txt)s, %(station_name)s, %(dest)s,
           %(journey_number)s, %(stop_id)s, %(time)s)
        RETURNING "abfahrt_id";
        '''
        cur = self.conn.cursor()
        cur.execute(sql,
                    dict(
                        url=journey['url'],
                        journey_name=journey['name'],
                        dt_txt=dt_txt,
                        station_name=route[0]['station_name'],
                        dest=journey['destination'],
                        journey_number=journey['number'] or None,
                        stop_id=stop_id,
                        time=self.date.strftime(self.sql_date_format),
                    )
                    )
        j_id = cur.fetchone()

        for i, section in enumerate(route):
            arr_time = section.get('arrival')
            at = (f"'{arr_time.strftime(self.sql_timestamp_format)}'"
                  if arr_time else None)
            dep_time = section.get('departure')
            dt = (f"'{dep_time.strftime(self.sql_timestamp_format)}'"
                  if dep_time else None)
            sql = f"""
            INSERT INTO "{self.schema}".fahrten
            (abfahrt_id, "Fahrt_Name", fahrt_index,
            "H_Name", "H_Ankunft", "H_Abfahrt", "H_ID")
            VALUES (%(journey_id)s, %(journey_name)s, %(index)s, %(station_name)s,
                    %(at)s, %(dt)s, %(station_id)s);
            """

            cur.execute(sql, vars=dict(
                journey_id=j_id[0],
                journey_name=journey['name'],
                index=i + 1,
                station_name=section['station_name'],
                at=at,
                dt=dt,
                station_id=section['station_id'],
            ))

    def check_journey(self, journey, stop_id):
        departure = journey['departure'].strftime(self.sql_time_format)
        sql = f"""
        SELECT 1 AS id
        FROM "{self.schema}".abfahrten AS a, "{self.schema}".fahrten AS f
        WHERE a.abfahrt_id = f.abfahrt_id
        AND a."Fahrt_Name" = %(journey_name)s
        AND f."H_ID" = %(stop_id)s
        AND f."H_Abfahrt"::time = %(departure)s
        AND a."Fahrt_Ziel" = %(journey_dest)s
        """
        cursor = self.conn.cursor()
        cursor.execute(sql, vars=dict(journey_name=journey['name'],
                                      stop_id=stop_id,
                                      departure=departure,
                                      journey_dest=journey['destination'],
                                      ))
        row = cursor.fetchone()
        return row is not None

    def add_missing_stops(self):
        """Add missing stops from master database to local database"""
        cursor = self.conn.cursor()

        sql = f"""
        SELECT DISTINCT("H_ID") FROM "{self.schema}".fahrten
        WHERE "H_ID" NOT IN (
        SELECT DISTINCT "H_ID" FROM "{self.schema}".haltestellen)
        """
        cursor.execute(sql)
        rows = cursor.fetchall()
        ids = [row[0] for row in rows]
        if len(ids) == 0:
            return
        self.create_foreign_schema()
        self.logger.info(f'Adding {len(ids)} missing stop(s) '
                         f'from database "{self.source_db}"...')
        chunksize = 1000
        for i in range(0, len(ids), chunksize):
            cur_ids = ids[i: i + chunksize]
            arr = ','.join([str(ci) for ci in cur_ids])
            sql = f"""
            INSERT INTO "{self.schema}".haltestellen
            ("H_ID", "H_Name", geom, kreis)
            SELECT "H_ID", "H_Name", st_transform(geom, {self.target_srid}) AS geom, kreis
            FROM "{self.temp_schema}".haltestellen
            WHERE "H_ID" = ANY(ARRAY[{arr}]);
            """
            self.run_query(sql, conn=self.conn)
        self.cleanup()


if __name__ == '__main__':

    parser = ArgumentParser(
        description="Scrape Stops in a given bounding box")

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
                        help="source database",
                        dest="source_db", default='europe')

    parser.add_argument('--date', action="store", type=str,
                        help="date in Format DD.MM.YYYY, default: today",
                        dest="date")

    parser.add_argument('--recreate-tables', action="store_true",
                        help="recreate the tables", default=False,
                        dest="recreate_tables")

    options = parser.parse_args()

    scrape = ScrapeTimetable(db=options.destination_db,
                             date=options.date,
                             source_db=options.source_db,
                             recreate_tables=options.recreate_tables)
    scrape.set_login(host=options.host, port=options.port, user=options.user)
    scrape.get_target_boundary_from_dest_db()
    scrape.scrape()

