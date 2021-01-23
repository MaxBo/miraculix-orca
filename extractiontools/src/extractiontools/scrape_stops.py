#!/usr/bin/env python
# coding:utf-8
import time
from argparse import ArgumentParser

from extractiontools.ausschnitt import Extract, Connection
from extractiontools.utils.bahn_query import BahnQuery
from psycopg2.extras import NamedTupleCursor
from typing import Tuple


class ScrapeStops(Extract):
    """Scrape Stops in bounding box"""
    tables = {}
    schema = 'timetables'
    role = 'group_osm'

    def scrape(self):
        """scrape stop from railway page"""
        with Connection(login=self.login) as conn:
            self.conn = conn
            self.create_schema(self.schema, conn=conn, replace=False)
            self.conn.commit()
            self.scrape_haltestellen()
            self.conn.commit()

    def additional_stuff(self):
        """
        """
        self.extract_table('haltestellen')
        self.copy_tables_to_target_db(self.schema, tables=['route_types'],
                                      conn=self.conn)

    def final_stuff(self):
        """"""
        self.create_index()

    def create_index(self):
        """
        CREATE INDEX
        """
        sql = f"""
        ALTER TABLE "{self.schema}".haltestellen ADD PRIMARY KEY ("H_ID");
        CREATE INDEX idx_haltestellen_geom
        ON "{self.schema}".haltestellen USING gist(geom);
        ALTER TABLE "{self.schema}".route_types ADD PRIMARY KEY (typ);
        """
        self.run_query(sql, self.conn)

    def get_cursor(self):
        """erzeuge Datenbankverbindung1 und Cursor"""
        cursor = self.conn.cursor()
        cursor.execute('SET search_path TO timetables, public')
        return cursor

    def scrape_haltestellen(self):
        """Lies Haltestellen und füge sie in DB ein bzw. aktualisiere sie"""

        point_distance = 14000
        search_radius = 10000
        points_with_too_many_stops = []

        sql = f'''
        CREATE TABLE IF NOT EXISTS {self.schema}.haltestellen (
        "H_Name" TEXT NOT NULL,
        "H_ID" INTEGER PRIMARY KEY,
        kreis TEXT,
        found BOOLEAN DEFAULT false NOT NULL,
        geom public.geometry,
        in_area BOOLEAN DEFAULT false
        );
        CREATE INDEX IF NOT EXISTS idx_haltestellen_geom
        ON "{self.schema}".haltestellen USING gist(geom);
        '''
        self.run_query(sql)

        sql = f'''
        SELECT
        st_x(b.point) x, st_y(b.point) y
        FROM (
          SELECT st_transform(st_centroid(a.geom),4326) point
          FROM (
            SELECT (ST_HexagonGrid({point_distance}, ST_Transform(geom, 3857))).*
            FROM meta.boundary
            WHERE name=%(boundary_name)s
          ) a
        ) b
        '''
        self.logger.info(sql)
        cursor = self.conn.cursor()
        cursor.execute(sql, {'boundary_name': self.boundary_name, })
        points = cursor.fetchall()

        db_query = BahnQuery(timeout=0.5)
        temp_table = 'temp_stations'

        for point in points:
            rowcount = self.get_stops_at_point(
                point, search_radius, db_query, cursor, temp_table)
            if rowcount >= 1000:
                points_with_too_many_stops.append(point)

        rel_step = 1
        for point in points_with_too_many_stops:
            x, y = point
            too_many_stops_found = True
            self.logger.info(f'search more stops around {point}')
            while too_many_stops_found:
                too_many_stops_found = False
                rel_step /= 2
                new_point_distance = point_distance * rel_step
                new_search_radius = search_radius * rel_step
                sql = f'''
                SELECT
                st_x(b.point) x, st_y(b.point) y
                FROM (
                  SELECT st_transform(st_centroid(a.geom),4326) point
                  FROM (
                    SELECT (ST_HexagonGrid(
                              {new_point_distance},
                              st_buffer(
                                ST_Transform(
                                  st_setsrid(
                                    st_makepoint({x}, {y}),
                                    4326),
                                  3857),
                                {search_radius})
                              )
                            ).*
                  ) a
                ) b
                '''
                self.logger.info(sql)
                cursor = self.conn.cursor()
                cursor.execute(sql)
                new_points = cursor.fetchall()

                for new_point in new_points:
                    rowcount = self.get_stops_at_point(new_point,
                                                       new_search_radius,
                                                       db_query,
                                                       cursor,
                                                       temp_table)
                    if rowcount >= 1000:
                        too_many_stops_found = True

        sql = f'DROP TABLE IF EXISTS "{self.schema}"."{temp_table}";'
        self.run_query(sql, verbose=False)

    def get_stops_at_point(self,
                           point: Tuple[float, float],
                           search_radius: float,
                           db_query: str,
                           cursor: NamedTupleCursor,
                           temp_table: str) -> int:
        x, y = point
        self.logger.debug(f'search at {x}, {y}')

        time.sleep(0.5)

        self.logger.info(f'Querying stations at {x}, {y}')

        stops = db_query.stops_near((x, y), max_distance=search_radius)
        if not stops:
            return 0
        sql = f'''
            DROP TABLE IF EXISTS "{self.schema}"."{temp_table}";
            CREATE TABLE IF NOT EXISTS "{self.schema}"."{temp_table}" (
            LIKE "{self.schema}".haltestellen INCLUDING CONSTRAINTS INCLUDING DEFAULTS);
            '''
        self.run_query(sql, verbose=False)
        for stop in stops:
            sql = f"""
                INSERT INTO "{self.schema}"."{temp_table}"
                ("H_Name", "H_ID", geom, in_area)
                VALUES
                (%(stop_name)s, %(stop_id)s,
                st_transform(
                  st_setsrid(st_makepoint( %(x)s, %(y)s), 4326 ), %(target_srid)s),
                True::boolean)
                """
            cursor.execute(sql,
                           {'stop_name': stop['name'],
                            'stop_id': stop['id'],
                            'x': stop['x'],
                            'y': stop['y'],
                            'target_srid': self.target_srid,
                            }
                           )

        sql = f'''
            DELETE FROM "{self.schema}"."{temp_table}" a
            USING meta.boundary b
            WHERE st_disjoint(a.geom, st_transform(b.source_geom, {self.target_srid}))
            AND b.name=%(boundary_name)s;
            '''
        cursor.execute(sql, {'boundary_name': self.boundary_name, })

        sql = f"""
            INSERT INTO "{self.schema}".haltestellen
            SELECT *
            FROM "{self.schema}"."{temp_table}" tt
            ON CONFLICT ("H_ID") DO UPDATE
            SET geom=excluded.geom,
                "H_Name"=excluded."H_Name",
                in_area=True::boolean;
            """
        self.logger.info(sql)
        cursor.execute(sql)

        self.logger.info(f'inserted or updated {cursor.rowcount} stops')
        self.conn.commit()
        return len(stops)


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
                        dest="destination_db")

    parser.add_argument('--no-copy', action="store_false", default=True,
                        help="don't copy from source database",
                        dest="copy_from_source_db")

    options = parser.parse_args()

    scrape = ScrapeStops(options, db=options.destination_db)
    scrape.set_login(host=options.host, port=options.port, user=options.user)
    if options.copy_from_source_db:
        scrape.extract()
    scrape.scrape()
