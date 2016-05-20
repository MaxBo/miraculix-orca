#!/usr/bin/env python
#coding:utf-8

import numpy as np
import logging
import time
from argparse import ArgumentParser
import urllib2
import random

from HTMLParser import HTMLParser
from lxml import html
import re, sets, requests, sys

import htmlentitydefs
htmlentitydefs.name2codepoint['apos'] = 39
htmlentitydefs.entitydefs['apos'] = '\x27'
htmlentitydefs.codepoint2name[39] = 'apos'

import os
import datetime
from extractiontools.ausschnitt import DBApp, Extract, Connection, BBox, logger


class ExtractStops(Extract):
    tables = {}
    schema = 'timetables'
    role = 'group_osm'
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


class ScrapeStops(Extract):
    schema = 'timetables'
    role = 'group_osm'

    def __init__(self,
                 options,
                 db='extract'):
        super(ScrapeStops, self).__init__(destination_db=db, options=options)
        self.db = db
        self.options = options

    def scrape(self):
        """"""
        with Connection(login=self.login1) as conn1:
            self.conn1 = conn1
            self.readHaltestellen()
            self.conn1.commit()

    def getSessionIDs(self):
        #SessionID von der Bahn bekommen
        ID_URL = 'http://mobile.bahn.de/bin/mobil/query2.exe/dox?country=DEU&rt=1&use_realtime_filter=1&stationNear=1)'
        ID_URL = self.urlquery(ID_URL)

        ID_URL1 = ID_URL.split('amp;i=')
        ID_URL1 = ID_URL1[1]
        ID_URL1 = ID_URL1.split('&amp;')
        ID_URL1 = ID_URL1[0]
        #print ID_URL1

        ID_URL2 = ID_URL.split('dox?ld=')
        ID_URL2 = ID_URL2[1]
        ID_URL2 = ID_URL2.split('&amp;')
        ID_URL2 = ID_URL2[0]
        #print ID_URL2
        return ID_URL1, ID_URL2

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

    def urlquery(self, url):
        # function cycles randomly through different user agents and time intervals to simulate more natural queries
        try:

            sleeptime = float(random.randint(1,5))/10
            time.sleep(sleeptime)
            agent = self.get_agent()

            opener = urllib2.build_opener()
            opener.addheaders = [('User-agent', agent)]
            #print agent

            html = opener.open(url).read()

            return html

        except:
            logger.warn("fehler in urlquery:")
            logger.warn(url)
            pass

    def getRequestsTree(self, url):
        """"""
        agent = self.get_agent()
        headers = {'User-Agent': agent}
        old_loglevel = logger.level
        logger.level = logging.WARN
        page = requests.get(url, headers=headers)
        logger.level = old_loglevel
        tree = html.fromstring(page.content)
        return tree

    def htmlentitydecode(self, s):
        try:
            u = s.decode('cp1252')
            for k,v in htmlentitydefs.entitydefs.items():
                if v.startswith('&'):
                    u = u.replace(v, unichr(htmlentitydefs.name2codepoint[k]))
            for k in htmlentitydefs.codepoint2name.keys():
                u = u.replace('&#%s;' %k, unichr(k))
        except:
            u=s
        return u

    def unescape(self, text):
        def fixup(m):
            text = m.group(0)
            if text[:2] == "&#":
                # character reference
                try:
                    if text[:3] == "&#x":
                        return unichr(int(text[3:-1], 16))
                    else:
                        return unichr(int(text[2:-1]))
                except ValueError:
                    pass
            else:
                # named entity
                try:
                    text = unichr(htmlentitydefs.name2codepoint[text[1:-1]])

                except KeyError:
                    pass
            return text # leave as is
        return re.sub("&#?\w+;", fixup, text)

    def get_cursor(self):
        """erzeuge Datenbankverbindung1 und Cursor"""
        cursor = self.conn1.cursor()
        cursor.execute('SET search_path TO timetables, public')
        return cursor

    def readHaltestellen(self):
        """Lies Haltestellen und füge sie in DB ein bzw. aktualisiere sie"""

        cursor = self.get_cursor()

        sql_update = """
    UPDATE haltestellen h
    SET "H_Name" = %s,
    geom = st_transform(st_setsrid(st_makepoint( %s, %s), 4326 ), %s::integer)
    WHERE h."H_ID" = %s;
                                """

        sql_insert = """
    INSERT INTO haltestellen
    ("H_Name", "H_ID", geom)
    SELECT
      %s,
      %s,
      st_transform(st_setsrid(st_makepoint( %s, %s), 4326 ),
      %s::integer)
    WHERE NOT EXISTS (SELECT 1 FROM haltestellen h WHERE h."H_ID" = %s);
                                """
        lon0, lon1, lat0, lat1 = self.bbox.rounded()
        stops_found = 0
        stops_inserted = 0
        for i in range(int(lat0*10), int(lat1*10)):
            for j in range(int(lon0*10),int(lon1*10)):
                stops_found_in_tile = 0
                stops_inserted_in_tile = 0

                time.sleep(0.5)

                lat = i * 100000
                lon = j * 100000

                lon = str(lon)
                lat = str(lat)

                logger.info('search in {}, {}'.format(i, j))

                ID_URL1, ID_URL2 = self.getSessionIDs()

                try:
                    # URL zusammensetzen
                    URL = ''.join(('http://mobile.bahn.de/bin/mobil/query2.exe/dox?ld=',
                                   str(ID_URL2),
                                   '&n=1&i=',
                                   ID_URL1,
                                   '&rt=1&use_realtime_filter=1&performLocating=2&tpl=stopsnear&look_maxdist=10000&look_stopclass=1023&look_x=', ))

                    URL = ''.join((URL, lon, "&look_y=", lat, "&"))
                except:
                    print 'fehler URL'
                    pass

                try:
                    #HTML auslesen

                    html = self.urlquery(URL)

                    html = html.split('<div class="overview clicktable">')
                    html = html[1]
                    html = html.split('<div class="bline">')
                    html = html[0]
                    html = html.split('<a class="uLine')

                    for element in html:

                        if len(element) < 80: x = 0
                        else:
                            element = element.split('</div>')
                            element = element[0]

                            #Haltestellenname
                            H_Name = element.split('>')
                            H_Name = H_Name[1]
                            H_Name = H_Name.replace('</a','')
                            H_Name = self.unescape(H_Name)

                            #HaltestellenID
                            H_ID = element.split('!id=')
                            H_ID = H_ID[1].split('!dist=')
                            H_ID = H_ID[0]

                            #HaltestellenLat
                            H_Lat = element.split('!Y=')
                            H_Lat = H_Lat[1].split('!id=')
                            H_Lat = float(H_Lat[0])
                            H_Lat /= 1000000.

                            #HaltestellenLat
                            H_Lon = element.split('!X=')
                            H_Lon = H_Lon[1].split('!Y=')
                            H_Lon = float(H_Lon[0])
                            H_Lon /= 1000000.

                            #Datenobjekt erzeugen und in DB schreiben
                            # wenn schon vorhanden, dann Geometrie aktualisieren

                            cursor.execute(sql_insert,
                                           (H_Name,
                                            H_ID,
                                            H_Lon, H_Lat,
                                            self.target_srid,
                                            H_ID))
                            stops_found += 1
                            stops_found_in_tile += 1
                            stops_inserted += cursor.rowcount
                            stops_inserted_in_tile += cursor.rowcount
                            if not cursor.rowcount:
                                # update name and geom if stop is already in db
                                cursor.execute(sql_update,
                                               (H_Name,
                                                H_Lon, H_Lat,
                                                self.target_srid,
                                                H_ID))

                            if not stops_found % 1000:
                                self.conn1.commit()

                except IndexError:
                    pass
                except TypeError:
                    pass

                logger.info(' found {} new stops'.format(
                    stops_inserted_in_tile))
                self.conn1.commit()

        logger.info('{} stops found and inserted'.format(
            stops_inserted))


if __name__=='__main__':


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

    if options.copy_from_source_db:
        extract = ExtractStops(destination_db=options.destination_db)
        extract.set_login(host=options.host, port=options.port, user=options.user)
        extract.get_target_boundary_from_dest_db()
        extract.extract()

    scrape = ScrapeStops(options, db=options.destination_db)
    scrape.set_login(host=options.host, port=options.port, user=options.user)
    scrape.get_target_boundary_from_dest_db()
    scrape.scrape()
