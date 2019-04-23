#!/usr/bin/env python
#coding:utf-8


import time
import os
import datetime
from argparse import ArgumentParser
import random

from urllib.parse import urlparse, parse_qs
import requests
from lxml import html

from extractiontools.ausschnitt import Extract, Connection, logger


class ScrapeStops(Extract):
    """Scrape Stops in bounding box"""
    tables = {}
    schema = 'timetables'
    role = 'group_osm'

    def scrape(self):
        """scrape stop from railway page"""
        with Connection(login=self.login1) as conn1:
            self.conn1 = conn1
            self.read_haltestellen()
            self.conn1.commit()

    @staticmethod
    def get_session_id():
        """get a session_id"""
        url = 'http://mobile.bahn.de/bin/mobil/query.exe/dox?'\
            'country=DEU&rt=1&use_realtime_filter=1&stationNear=1)'
        r = requests.get(url)
        tree = html.fromstring(r.content)
        elems = tree.xpath('/html/body/div/div[2]/div/div/div/form')
        if not elems:
            logger.warning('connection failed, no valid response')

        elem = elems[0]
        o = urlparse(elem.action)
        query = (o.query)
        try:
            id2 = query['ld'][0]
            id1 = query['i'][0]
        except (KeyError, IndexError):
            logger.warning('no valid response')

        return id1, id2

    def get_agent(self):
        """
        return a random agent
        """
        agents = ['Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_2) AppleWebKit/537.17 (KHTML, like Gecko) Chrome/24.0.1309.0 Safari/537.17',
                  'Mozilla/5.0 (compatible; MSIE 10.6; Windows NT 6.1; Trident/5.0; InfoPath.2; SLCC1; .NET CLR 3.0.4506.2152; .NET CLR 3.5.30729; .NET CLR 2.0.50727) 3gpp-gba UNTRUSTED/1.0',
                  'Opera/12.80 (Windows NT 5.1; U; en) Presto/2.10.289 Version/12.02',
                  'Mozilla/4.0 (compatible; MSIE 5.5; Windows NT)',
                  'Mozilla/3.0',
                  'Mozilla/5.0 (iPhone; U; CPU like Mac OS X; en) AppleWebKit/420+ (KHTML, like Gecko) Version/3.0 Mobile/1A543a Safari/419.3',
                  'Mozilla/5.0 (Linux; U; Android 0.5; en-us) AppleWebKit/522+ (KHTML, like Gecko) Safari/419.3',
                  'Opera/9.00 (Windows NT 5.1; U; en)']

        agent = random.choice(agents)
        return agent

    def additional_stuff(self):
        """
        """
        self.extract_table('haltestellen')
        self.copy_route_types()

    def final_stuff(self):
        """"""
        self.create_index()

    def create_index(self):
        """
        CREATE INDEX
        """
        sql = """
ALTER TABLE {schema}.haltestellen ADD PRIMARY KEY ("H_ID");
CREATE INDEX idx_haltestellen_geom
ON {schema}.haltestellen USING gist(geom);
ALTER TABLE {schema}.route_types ADD PRIMARY KEY (typ);
            """.format(schema=self.schema)
        self.run_query(sql, self.conn1)

    def copy_route_types(self):
        """"""
        sql = """

CREATE TABLE {temp}.route_types
(
  route_type integer NOT NULL,
  name text,
  typ text NOT NULL
);

INSERT INTO {temp}.route_types
SELECT *
FROM {schema}.route_types;
        """
        self.run_query(sql.format(temp=self.temp, schema=self.schema),
                       conn=self.conn0)

    def get_cursor(self):
        """erzeuge Datenbankverbindung1 und Cursor"""
        cursor = self.conn1.cursor()
        cursor.execute('SET search_path TO timetables, public')
        return cursor

    def read_haltestellen(self):
        """Lies Haltestellen und füge sie in DB ein bzw. aktualisiere sie"""

        cursor = self.get_cursor()

        sql_update = """
    UPDATE haltestellen h
    SET "H_Name" = %s,
    geom = st_transform(st_setsrid(st_makepoint( %s, %s), 4326 ), %s::integer),
    in_area=True::boolean
    WHERE h."H_ID" = %s;
                                """

        sql_insert = """
    INSERT INTO haltestellen
    ("H_Name", "H_ID", geom, in_area)
    SELECT
      %s,
      %s,
      st_transform(st_setsrid(st_makepoint( %s, %s), 4326 ),
      %s::integer),
      True::boolean
    WHERE NOT EXISTS (SELECT 1 FROM haltestellen h WHERE h."H_ID" = %s);
                                """
        lon0, lon1, lat0, lat1 = self.bbox.rounded()
        stops_found = 0
        stops_inserted = 0
        for j in range(int(lat0*10), int(lat1*10)):
            for i in range(int(lon0*10),int(lon1*10)):
                stops_found_in_tile = 0
                stops_inserted_in_tile = 0

                time.sleep(0.5)

                lat = j * 100000
                lon = i * 100000


                logger.info(f'search in {i}, {j}')

                id1, id2 = self.get_session_id()

                try:
                    # URL zusammensetzen
                    url = (
                        f'http://mobile.bahn.de/bin/mobil/query.exe/dox?'
                        f'ld={id2}&n=1&i={id1}&rt=1&use_realtime_filter=1&'
                        f'performLocating=2&tpl=stopsnear&look_maxdist=10000&'
                        f'look_stopclass=1023&look_x={lon}&look_y={lat}&'
                    )
                except:
                    print('fehler URL')
                    pass

                try:
                    r = requests.get(url) # agent...
                    tree = html.fromstring(r.content)
                    #HTML auslesen

                    overview_clicktable = '//div[@class="overview clicktable"]/*'
                    elems = tree.xpath(overview_clicktable)
                    if not elems:
                        logger.warning('connection failed, no valid response')

                    for elem in elems:
                        link = elem.xpath('a')[0]
                        h_name = link.text
                        href = link.get('href')
                        o = urlparse(href)
                        station_query = parse_qs(o.query)['HWAI'][0]
                        station_params = {}
                        for param in station_query.split('!'):
                            param_tuple = param.split('=')
                            if len(param_tuple) > 1:
                                station_params[param_tuple[0]] = param_tuple[1]
                        #HaltestellenID
                        h_id = station_params['id']
                        h_lat = float(station_params['Y']) / 1000000.
                        h_lon = float(station_params['X']) / 1000000.

                        #Datenobjekt erzeugen und in DB schreiben
                        # wenn schon vorhanden, dann Geometrie aktualisieren

                        cursor.execute(sql_insert,
                                       (h_name,
                                        h_id,
                                        h_lon, h_lat,
                                        self.target_srid,
                                        h_id))
                        stops_found += 1
                        stops_found_in_tile += 1
                        stops_inserted += cursor.rowcount
                        stops_inserted_in_tile += cursor.rowcount
                        if not cursor.rowcount:
                            # update name and geom if stop is already in db
                            cursor.execute(sql_update,
                                           (h_name,
                                            h_lon, h_lat,
                                            self.target_srid,
                                            h_id))

                        if not stops_found % 1000:
                            self.conn1.commit()

                except IndexError:
                    pass
                except TypeError:
                    pass

                logger.info(f' found {stops_inserted_in_tile} new stops')
                self.conn1.commit()

        logger.info(f'{stops_inserted} stops found and inserted')


if __name__ == '__main__':


    parser = ArgumentParser(description="Scrape Stops in a given bounding box")

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
