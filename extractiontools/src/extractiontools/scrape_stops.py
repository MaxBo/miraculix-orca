#!/usr/bin/env python
# coding:utf-8
import time
from argparse import ArgumentParser

from extractiontools.ausschnitt import Extract, Connection
from extractiontools.utils.bahn_query import BahnQuery


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
        ALTER TABLE {self.schema}.haltestellen ADD PRIMARY KEY ("H_ID");
        CREATE INDEX idx_haltestellen_geom
        ON {self.schema}.haltestellen USING gist(geom);
        ALTER TABLE {self.schema}.route_types ADD PRIMARY KEY (typ);
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
        sql = f'''
        CREATE TABLE IF NOT EXISTS {self.schema}.haltestellen (
        "H_Name" TEXT NOT NULL,
        "H_ID" INTEGER NOT NULL,
        kreis TEXT,
        found BOOLEAN DEFAULT false NOT NULL,
        geom public.geometry,
        in_area BOOLEAN DEFAULT false
        );
        '''
        self.run_query(sql)

        sql = f'''
        SELECT
        st_x(b.point) x, st_y(b.point) y
        FROM (SELECT st_transform(st_centroid(a.geom),4326) point
        FROM ( SELECT (ST_HexagonGrid({point_distance}, ST_Transform(geom, 3857))).*
        FROM meta.boundary WHERE name='{self.boundary_name}') a ) b
        '''
        cursor = self.conn.cursor()
        self.logger.info(sql)
        cursor.execute(sql)
        points = cursor.fetchall()

        stops_found = 0
        stops_inserted = 0
        db_query = BahnQuery(timeout=0.5)
        for x, y in points:
            self.logger.debug(f'search at {x}, {y}')
            stops_found_in_tile = 0
            stops_inserted_in_tile = 0

            time.sleep(0.5)

            self.logger.info(f'Querying stations at {x}, {y}')

            stops = db_query.stops_near((x, y), max_distance=search_radius)
            for stop in stops:
                sql = f"""
                INSERT INTO {self.schema}.haltestellen
                ("H_Name", "H_ID", geom, in_area)
                SELECT
                  '{stop['name']}', {stop['id']},
                  st_transform(st_setsrid(st_makepoint( {stop['x']}, {stop['y']}), 4326 ),
                              {self.target_srid}::integer),
                  True::boolean
                WHERE NOT EXISTS (SELECT 1 FROM {self.schema}.haltestellen h WHERE h."H_ID" = {stop['id']});
                """
                self.logger.info(sql)
                cursor.execute(sql)
                stops_found += 1
                stops_found_in_tile += 1
                stops_inserted += cursor.rowcount
                stops_inserted_in_tile += cursor.rowcount
                if not cursor.rowcount:
                    # update name and geom if stop is already in db
                    sql = f"""
                    UPDATE {self.schema}.haltestellen h
                    SET "H_Name" = '{stop['name']}',
                    geom = st_transform(st_setsrid(st_makepoint( {stop['x']}, {stop['y']}), 4326 ),
                                        {self.target_srid}::integer),
                    in_area=True::boolean
                    WHERE h."H_ID" = {stop['id']};
                    """
                    self.logger.info(sql)
                    cursor.execute(sql)

                if not stops_found % 1000:
                    self.conn.commit()

            self.logger.info(f' found {stops_inserted_in_tile} new stops')
            self.conn.commit()

        self.logger.info(f'{stops_inserted} stops found and inserted')


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
    scrape.get_target_boundary_from_dest_db()
    if options.copy_from_source_db:
        scrape.extract()
    scrape.scrape()
