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

        cursor = self.conn.cursor()
        sql = f'''
        SELECT EXISTS (
        SELECT FROM information_schema.tables
        WHERE  table_schema = '{self.schema}'
        AND    table_name   = 'haltestellen'
        );
        '''
        cursor.execute(sql)
        row = cursor.fetchone()
        exists = row[0]

        if not exists:
            sql = f'''
            CREATE TABLE {self.schema}.haltestellen (
            "H_Name" TEXT NOT NULL,
            "H_ID" INTEGER PRIMARY KEY,
            kreis TEXT,
            found BOOLEAN DEFAULT false NOT NULL,
            geom public.geometry,
            in_area BOOLEAN DEFAULT false
            );
            CREATE INDEX idx_haltestellen_geom
            ON {self.schema}.haltestellen USING gist(geom);
            '''
            self.run_query(sql)

        sql = f'''
        SELECT
        st_x(b.point) x, st_y(b.point) y
        FROM (SELECT st_transform(st_centroid(a.geom),4326) point
        FROM ( SELECT (ST_HexagonGrid({point_distance}, ST_Transform(geom, 3857))).*
        FROM meta.boundary WHERE name='{self.boundary_name}') a ) b
        '''
        self.logger.info(sql)
        cursor.execute(sql)
        points = cursor.fetchall()

        db_query = BahnQuery(timeout=0.5)

        for x, y in points:
            self.logger.debug(f'search at {x}, {y}')

            time.sleep(0.5)

            self.logger.info(f'Querying stations at {x}, {y}')

            stops = db_query.stops_near((x, y), max_distance=search_radius)
            if not stops:
                continue
            temp_table = 'temp_stations'
            sql = f'''
            DROP TABLE IF EXISTS {self.schema}.{temp_table};
            CREATE TABLE IF NOT EXISTS {self.schema}.{temp_table} (
            LIKE {self.schema}.haltestellen INCLUDING CONSTRAINTS INCLUDING DEFAULTS);
            '''
            self.run_query(sql, verbose=False)
            for stop in stops:
                sql = f"""
                INSERT INTO {self.schema}.{temp_table}
                ("H_Name", "H_ID", geom, in_area)
                VALUES
                ('{stop['name']}', {stop['id']},
                st_transform(st_setsrid(st_makepoint( {stop['x']}, {stop['y']}), 4326 ),
                            {self.target_srid}::integer),
                True::boolean)
                """
                cursor.execute(sql)

            sql = f'''
            DELETE FROM {self.schema}.{temp_table} a
            USING meta.boundary b
            WHERE st_disjoint(a.geom, st_transform(b.source_geom, {self.target_srid}))
            AND b.name='{self.boundary_name}';
            '''
            cursor.execute(sql)

            sql = f"""
            INSERT INTO {self.schema}.haltestellen
            SELECT *
            FROM {self.schema}.{temp_table} tt
            ON CONFLICT ("H_ID") DO UPDATE
            SET geom=excluded.geom, "H_Name"=excluded."H_Name", in_area=True::boolean;
            """
            self.logger.info(sql)
            cursor.execute(sql)

            self.logger.info(f'inserted or updated {cursor.rowcount} stops')
            self.conn.commit()
        sql = f'DROP TABLE IF EXISTS {self.schema}.{temp_table};'
        self.run_query(sql, verbose=False)

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
