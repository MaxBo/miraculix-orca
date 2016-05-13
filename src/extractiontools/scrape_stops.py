#!/usr/bin/env python
#coding:utf-8

import numpy as np
import logging
import time
logger = logging.getLogger()
logger.addHandler(logging.StreamHandler())
logger.level = logging.DEBUG
import os
import datetime
from extractiontools.ausschnitt import Extract, Connection


class ScrapeStops(Extract):
    tables = {}
    schema = 'timetables'
    role = 'group_osm'

    def getSessionID(self):
        #SessionID von der Bahn bekommen
        ID_URL = 'http://mobile.bahn.de/bin/mobil/query2.exe/dox?country=DEU&rt=1&use_realtime_filter=1&stationNear=1)'
        ID_URL = urlquery(ID_URL)

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

    def additional_stuff(self):
        """
        """
        self.extract_table('haltestellen')
        self.readHaltestellen()


    def readHaltestellen(self):

        #erzeuge Datenbankverbindung1 und Cursor

        try:
            cursor = self.conn1.cursor()
            cursor.execute('SET search_path TO timetables, public')

            lon0, lon1, lat0, lat1 = self.bbox.rounded()
            ii = 0
            for i in range(int(lat0*10), int(lat1*10)):
                for j in range(int(lon0*10),int(lon1*10)):

                    time.sleep(0.5)

                    lat = i*100000
                    lon = j*100000

                    lon = str(lon)
                    lat = str(lat)

                    print lat,lon

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

                        html = urlquery(URL)

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
                                H_Name = unescape(H_Name)

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
                                sql2 = """INSERT INTO haltestellen ("H_Name", "H_ID", geom)""" + """
                                (SELECT %s, %s, st_setsrid(st_makepoint( %s, %s), 4326 )
                                WHERE NOT EXISTS
                                """ + """(SELECT 1 FROM haltestellen """  +"""WHERE "H_ID" = %s));
                                """
                                cursor.execute(sql2, (H_Name, H_ID, H_Lon, H_Lat, H_ID))
                                ii += 1
                                if not ii % 100:
                                    self.conn1.connection, commit()
                                #if READ_FAHRTEN:
                                    #readFahrten(H_ID, cursor, date)

                    except IndexError:
                        pass
                    except TypeError:
                        pass


if __name__=='__main__':


    parser = ArgumentParser(description="Scrape Stops in a given bounding box")

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
    parser.add_argument('--source-db', action="store",
                        help="source database",
                        dest="source_db", default='europe')

    options = parser.parse_args()

    bbox = BBox(top=options.top, bottom=options.bottom,
                left=options.left, right=options.right)
    extract = ScrapeStops(source_db=options.source_db,
                         destination_db=options.destination_db,
                         target_srid=options.srid,
                         recreate_db=False)
    extract.set_login(host=options.host, port=options.port, user=options.user)
    extract.get_target_boundary_from_dest_db()
