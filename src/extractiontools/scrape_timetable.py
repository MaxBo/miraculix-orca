#!/usr/bin/env python
#coding:utf-8

from argparse import ArgumentParser

import numpy as np
import logging
import os
import sys
import datetime
import random
import time
from extractiontools.scrape_stops import ScrapeStops, logger, Connection, logger
from extractiontools.utils.get_date import Date, get_timestamp2
from HTMLParser import HTMLParser

class ScrapeTimetable(ScrapeStops):

    def __init__(self, options, db):
        """"""
        super(ScrapeTimetable, self).__init__(options, db=db)
        self.set_date(options.date)
        self.recreate_tables = options.recreate_tables

    def set_date(self, date):
        """
        """
        self.date = Date.from_string(date)

    def scrape(self):
        """"""
        with Connection(login=self.login1) as conn1:
            self.conn1 = conn1
            self.create_timetable_tables()
            if self.recreate_tables:
                self.truncate_timetables()
            self.conn1.commit()

            self.get_fahrten_for_stops()
            self.conn1.commit()
            self.add_missing_stops()
            self.conn1.commit()

    def create_timetable_tables(self):
        """(Re-Create the timetable tables)"""
        sql = """

CREATE TABLE IF NOT EXISTS abfahrten
(
  "Fahrt_URL" text,
  "Fahrt_Name" text,
  "Fahrt_Abfahrt" timestamp(0) with time zone,
  "H_ID" integer,
  abfahrt_id bigint NOT NULL,
  "Fahrt_Ziel" text,
  CONSTRAINT abfahrten_pkey PRIMARY KEY (abfahrt_id),
  CONSTRAINT abfahrten_fk FOREIGN KEY ("H_ID")
      REFERENCES haltestellen ("H_ID") MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE NO ACTION
);

CREATE INDEX IF NOT EXISTS abfahrten_sh_idx
  ON abfahrten
  USING btree
  ("Fahrt_Name", "Fahrt_Ziel");

CREATE TABLE IF NOT EXISTS fahrten
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
      REFERENCES abfahrten (abfahrt_id) MATCH SIMPLE
      ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS fahrten_idx1
  ON fahrten
  USING btree
  ("H_Name", "H_Abfahrt");
        """
        cursor = self.get_cursor()
        cursor.execute(sql)

    def truncate_timetables(self):
        """Truncate the timetables"""
        sql = """
TRUNCATE abfahrten CASCADE;
TRUNCATE fahrten;
        """
        cursor = self.get_cursor()
        cursor.execute(sql)

    def get_fahrten_for_stops(self):
        """get the stops in the area"""
        # testdata: Lübeck: 8000237 (> 1000 Abfahrten)
        # Bremen, Cranzer Straße: 627106 (No Abfahrten)
        sql = """
SELECT "H_ID", "H_Name"
FROM haltestellen
--WHERE "H_ID" = 692757;
        """
        cursor = self.get_cursor()
        cursor.execute(sql)
        rows = cursor.fetchall()
        i = 0
        for row in rows:
            H_ID = row[0]
            H_Name = row[1].decode('utf8')
            i += 1
            #logger.info(u'{}: {}: {}'.format(i, H_ID, H_Name))
            self.readFahrten(
                H_ID_Abfahrtstafel=H_ID,
                H_Name_Abfahrtstafel=H_Name)
            if not i % 1:
                self.conn1.commit()
        self.conn1.commit()

    def readFahrten(self,
                    H_ID_Abfahrtstafel,
                    H_Name_Abfahrtstafel):
        """Lies Fahrten"""
        cursor = self.get_cursor()

        sql = """SELECT max(abfahrt_id) FROM abfahrten"""
        cursor.execute(sql)
        rows = cursor.fetchall()
        self.abfahrt_id = rows[0][0] or 0

        #Abfahrten auslesen
        H_IDstr = str(H_ID_Abfahrtstafel)

        # grab data from station timetable for the whole day
        # use 24:00 instead of 00:00 to really get all departures also on stations
        # with a lot of departures
        bhftafel_URL = 'http://reiseauskunft.bahn.de/bin/bhftafel.exe/dn?ld=96242&country=DEU&rt=1&evaId=%s&bt=dep&time=24:00&maxJourneys=10000&date=%s&productsFilter=1111111111&max=10000&start=yes' %(
            H_IDstr, self.date)

        try:
            # wait a bit
            sleeptime = random.randint(1, 4)
            time.sleep(sleeptime)
            # try first time to get the Bahnhofstafel
            tree = self.getRequestsTree(bhftafel_URL)
            errCode = tree.xpath('//div[@class="hafasContent error"]/text()')

            MAX_TRIES = 10
            t = 0
            while t < MAX_TRIES and errCode:
                # if not successful,
                # try to get the page MAX_TRIES times
                WAITTIME = 15
                logger.info('wait {} secs and try again {} for url {}'.format(
                    WAITTIME, t, bhftafel_URL))
                time.sleep(WAITTIME)
                tree = self.getRequestsTree(bhftafel_URL)
                errCode = tree.xpath('//div[@class="hafasContent error"]/text()')
                t += 1
            try:
                subtree = tree.xpath('//*[@id="sqResult"]/table')
                # count number of trips
                elements = subtree[0].findall("tr")
                # only elements with a attribute "key"
                journey_rows = [e for e in elements if e.attrib.has_key('id')]
                fahrten_count = len(journey_rows)

            except:
                errCode = tree.xpath('//div[@class="errormsg leftMargin"]/text()')
                logger.warn(u'fehler beim Lesen der BhfTafel für {}: {}:'.format(
                    H_IDstr, H_Name_Abfahrtstafel))
                if errCode:
                    logger.warn(errCode[0])
                return
            if fahrten_count:
                logger.info(u'{} abfahrten in {}: {}'.format(fahrten_count,
                                                            H_IDstr,
                                                            H_Name_Abfahrtstafel))
                self.n_new = 0
                self.n_already_in_db = 0
                self.last_stunde = '-1'
                journeys = (j.attrib['id'] for j in journey_rows)
                for journey in journeys:
                    self.parse_fahrten(journey,
                                       fahrten_count,
                                       tree,
                                       H_ID_Abfahrtstafel,
                                       cursor,
                                       H_Name_Abfahrtstafel)
                if self.n_new:
                    logger.info('')
                logger.info(u'{} new, {} already in db'.format(
                    self.n_new,
                    self.n_already_in_db))
        except Exception:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            logger.warn('{} {} {}'.format(
                exc_type, fname, exc_tb.tb_lineno))

    def parse_fahrten(self,
                      journey,
                      fahrten_count,
                      tree,
                      H_ID_Abfahrtstafel,
                      cursor,
                      H_Name_Abfahrtstafel):
        """
        Parse Fahrt journey in the tree and query the Fahrtverlauf
        look in Fahrten, if Fahrt already exists
        """
        try:
            xpath_base = '//*[@id="{}"]'.format(journey)
            try:
                Fahrt_Ziel = tree.xpath(xpath_base+'/td[4]/span/a/text()')[0].replace('\n','')
                Abfahrten = tree.xpath(xpath_base+'/td[4]/text()')[2:][0].split('\n')
            except IndexError:
                logger.warn('Fehler beim parsen der Abfahrtstafel von {}'.format(
                    H_Name_Abfahrtstafel))
            Abfahrtshaltestelle = Abfahrten[1].strip()
            Abfahrtsuhrzeit = Abfahrten[2]
            Ankunftsuhrzeit = Abfahrten[-2]
            Fahrt_Abfahrt = time.strptime(Abfahrtsuhrzeit, '%H:%M')
            Fahrt_Name = tree.xpath(xpath_base+'//td[3]/a/text()')[0].split('\n')[1]

            Fahrt_URL = 'http://reiseauskunft.bahn.de/'+tree.xpath(xpath_base+'//td[3]/a/@href')[0]

            if Fahrt_URL:
                hstID_Abfahrt = int(Fahrt_URL.split('station_evaId=')[1].split('&')[0])
            else:
                hstID_Abfahrt = H_ID_Abfahrtstafel

        except:
            # if there is an error, wait
            logger.warn('fehler beim Auslesen der BHF-Tafel')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            logger.warn(exc_type, fname, exc_tb.tb_lineno)
            time.sleep(10)
            return

        try:
            sql = """
SELECT 1 AS id
FROM abfahrten AS a, fahrten AS f
WHERE a.abfahrt_id = f.abfahrt_id
AND a."Fahrt_Name" = %s AND f."H_Name" = %s
AND f."H_Abfahrt" = %s AND a."Fahrt_Ziel" = %s """

            cursor.execute(sql,
                           (Fahrt_Name,
                           Abfahrtshaltestelle,
                           self.date.get_timestamp(Fahrt_Abfahrt),
                           Fahrt_Ziel))
            rows = cursor.fetchall()
            if not rows:
                self.n_new += 1
                self.abfahrt_id += 1

                stunde = Abfahrtsuhrzeit[:2]
                if stunde != self.last_stunde:
                    self.last_stunde = stunde
                    print os.linesep, stunde, ':',

                #Fahrten in Tabelle Abfahrten schreiben
                sql1 = """
INSERT INTO abfahrten
(abfahrt_id, "Fahrt_URL", "Fahrt_Name", "Fahrt_Abfahrt", "H_ID", "Fahrt_Ziel")
VALUES (%s, %s, %s, %s, %s, %s);"""
                cursor.execute(sql1,
                               (self.abfahrt_id,
                                Fahrt_URL,
                                Fahrt_Name,
                                self.date.get_timestamp(Fahrt_Abfahrt),
                                H_ID_Abfahrtstafel,
                                Fahrt_Ziel))
                print u'{}'.format(Fahrt_Name),
                try:
                    sleeptime = float(random.randint(1,3))/20.
                    time.sleep(sleeptime)
                    Fahrtverlauf = self.urlquery(Fahrt_URL)
                except:
                    logger.warn('Fehler in Abfrage des Fahrtverlaufs')
                    pass

                try:
                    VerlaufParser = MyHTMLParser()
                    VerlaufParser.query_date = self.date
                except:
                    logger.warn('Fehler bei VerlaufParser')
                    pass
                # wenn Uhrzeit zwischen 0 und 4 h und Ankunft vor 4 Uhr ist,
                # wird der Vortag als "Zuglauf" ausgegeben
                # dies muss korrigiert werden
                VerlaufParser.Ankunft_vor_4Uhr = int(Ankunftsuhrzeit[:2]) < 4
                VerlaufParser.ist_Starthaltestelle = True

                try:
                    html_verlauf = self.htmlentitydecode(Fahrtverlauf)
                except:
                    logger.warn('HTML Verlauf Fehler')
                    pass
                VerlaufParser.feed(html_verlauf)
                for h in xrange(len(VerlaufParser.data_stations)):
                    H_Name = VerlaufParser.data_stations[h]
                    #print VerlaufParser.data_arrivals ##
                    H_Ankunft = VerlaufParser.data_arrivals[h]
                    H_Abfahrt = VerlaufParser.data_departures[h]

                    if H_Name == H_Name_Abfahrtstafel:
                        akt_H_ID = H_ID_Abfahrtstafel
                    else:
                        akt_H_ID = None


                    sql3 = """
INSERT INTO fahrten
(abfahrt_id, "Fahrt_Name", fahrt_index, "H_Name", "H_Ankunft", "H_Abfahrt", "H_ID")
VALUES (%s, %s, %s,%s,%s,%s,%s);"""
                    cursor.execute(sql3,
                                   (self.abfahrt_id,
                                    Fahrt_Name,
                                    h+1,
                                    H_Name,
                                    get_timestamp2(H_Ankunft),
                                    get_timestamp2(H_Abfahrt),
                                    akt_H_ID))
                VerlaufParser.close()

            else:
                # wenn Fahrt schon vorhanden
                self.n_already_in_db += 1
                sql = """
UPDATE fahrten AS f
SET "H_ID" = %s
FROM abfahrten AS a
WHERE a.abfahrt_id = f.abfahrt_id
AND f."H_ID" IS NULL
AND a."Fahrt_Name" = %s AND f."H_Name" = %s
AND f."H_Abfahrt" = %s AND a."Fahrt_Ziel" = %s """
                cursor.execute(sql,
                               (H_ID_Abfahrtstafel,
                                Fahrt_Name,
                                H_Name_Abfahrtstafel,
                                self.date.get_timestamp(Fahrt_Abfahrt),
                                Fahrt_Ziel))
        except Exception:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            logger.warn('{} {} {}'.format(
                exc_type, fname, exc_tb.tb_lineno))
            pass

    def add_missing_stops(self):
        """Add missing stops from master database to local database"""
        cur = self.conn1.cursor()

        sql = """
    SELECT dblink_connect_u('conn', 'dbname={sd}');
    -- füge fehlende Haltestelle aus der Deutschland-Tabelle hinzu
    INSERT INTO {s}.haltestellen
    ("H_ID", "H_Name", geom, kreis)
    SELECT "H_ID", "H_Name", st_transform(geom, {srid}) AS geom, kreis
    FROM dblink('conn',
    'SELECT h."H_ID", h."H_Name", h.geom, h.kreis
    FROM {s}.haltestellen AS h') AS hd(
    "H_ID" integer,
    "H_Name" text,
    geom geometry,
    kreis text)
    WHERE hd."H_ID" NOT IN (SELECT DISTINCT h."H_ID" FROM {s}.haltestellen h)
    AND hd."H_Name" IN (SELECT DISTINCT f."H_Name" FROM {s}.fahrten AS f);
    """

        query = sql.format(srid=self.target_srid,
                           sd=self.options.source_db,
                           s=self.schema)
        logger.info(query)
        cur.execute(query)
        logger.info('{msg}'.format(msg=cur.statusmessage))


class MyHTMLParser(HTMLParser):
    """create a subclass and override the handler methods"""
    def __init__(self):
        HTMLParser.__init__(self)
        self.recording_route = 0
        self.recording_train = 0
        self.data_route = []
        self.data_train = []
        self.links = []
        self.start_recording_links = 0
        self.recording_station = 0
        self.recording_arrival = 0
        self.recording_departure = 0
        self.recording_trainroute = 0
        self.data_stations = []
        self.data_arrivals = []
        self.data_departures = []
        self.first_linefeed = True

    def handle_starttag(self, tag, attrs):
        if tag == 'td':
            if attrs:
                #print attrs[0][1] ##
                if attrs[0][1] == 'route':
                    self.recording_route = 1
                elif attrs[0][1] == 'train':
                    self.recording_train = 1
                    self.start_recording_links = 1
                elif attrs[0][1] == 'station':
                    self.recording_station = 1
                elif attrs[0][1] == 'arrival nowrap':##
                    self.recording_arrival = 1
                elif attrs[0][1] == 'departure nowrap':##
                    self.recording_departure = 1

        elif tag == 'a':
            if self.start_recording_links:
                for (key, value) in attrs:
                    if key == 'href':
                        if value.startswith('http:'):
                            self.links.append(value)

        elif tag == 'h3':
            if attrs[0][1] == 'trainroute':
                self.recording_trainroute = 1

    def handle_endtag(self, tag):
        if tag == 'td':
            self.recording_route = 0
            self.recording_train = 0
            self.recording_station = 0
            self.recording_arrival = 0
            self.recording_departure = 0
        if tag == 'h3':
            self.recording_trainroute = 0


    def handle_data(self, data):
        if self.recording_route:
            if data <> '\n':
                self.data_route.append(data)
        if self.recording_trainroute:
            if data <> '\n':
                date = data.split('Fahrtverlauf vom: ')[1].rstrip(')')
                d, m, y = date.split('.')
                yyyy = int(y) + 2000
                self.date = Date(yyyy, m, d)
        elif self.recording_train:
            if data <> '\n':
                self.data_train.append(data)
        elif self.recording_station:
            if data <> '\n':
                self.data_stations.append(data)
        elif self.recording_arrival:
            if data == '\n':
                if self.first_linefeed:
                    zeit = None
                    self.data_arrivals.append(zeit)
                    self.first_linefeed = False
                else:
                    self.first_linefeed = True
            else:
                try:
                    # if tag is Delay marker
                    if data.startswith(u'+'):
                        return
                    zeit = self.get_time(data)
                    if self.data_departures:
                        z = len(self.data_departures)-1
                        while z >= 0:
                            abfahrtszeit = self.data_departures[z]
                            if abfahrtszeit:
                                if zeit < abfahrtszeit:
                                    # setze auf Folgetag
                                    self.date = self.date.shift_day(1)
                                    zeit = self.get_time(data)
                                break
                            z -= 1
                except Exception as F:
                    logger.warning(F)
                    zeit = None
                    raise
                self.data_arrivals.append(zeit)
        elif self.recording_departure:
            if data == '\n':
                if self.first_linefeed:
                    zeit = None
                    self.data_departures.append(zeit)
                    self.first_linefeed = False
                else:
                    self.first_linefeed = True

            # Delay Marker
            elif data.startswith(u'+'):
                return
            # normal time
            else:
                try:
                    if self.ist_Starthaltestelle:
                        if len(data.strip()) > 0:
                            # wenn Abfahrt an der Starthaltestelle vor 4 Uhr
                            if int(data.strip()[:2]) < 4:
                                # und Ankunft an der Starthaltestelle vor 4 Uhr
                                if self.Ankunft_vor_4Uhr:
                                    # korrigiere das Datum auf den Tag der Anfrage
                                    self.date = self.query_date
                        self.ist_Starthaltestelle = False
                    zeit = self.get_time(data)
                    if self.data_arrivals[-1]:
                        if zeit < self.data_arrivals[-1]:
                            self.date = self.date.shift_day(1)
                            zeit = self.get_time(data)
                    else:
                        # wenn keine Ankunftszeit angegeben ist, schaue, ob Zeitsprung an Abfahrt an vorheriger Haltestelle
                        z = len(self.data_departures)-1
                        while z >= 0:
                            abfahrtszeit = self.data_departures[z]
                            if abfahrtszeit:
                                if zeit < abfahrtszeit:
                                    # setze auf Folgetag
                                    self.date = self.date.shift_day(1)
                                    zeit = self.get_time(data)
                                break
                            z -= 1
                except:
                    zeit = None
                self.data_departures.append(zeit)

    def get_time(self, data):
        """
        return a Time-object with the current date from the time given as string
        in the data Parameter

        Parameters
        ----------
        data : str
            the time in format '14:23'

        Returns
        -------
        zeit : time.struct_time instance

        """
        try:
            zeit = time.strptime('{t} {d}'.format(t=data.strip(), d=self.date),
                                 '%H:%M %d.%m.%y')
        except ValueError as e:
            logger.warn(
                '"{}" could not be processed by time.strptime'.format(data))
            raise e
        return zeit


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

    scrape = ScrapeTimetable(options, options.destination_db)
    scrape.set_login(host=options.host, port=options.port, user=options.user)
    scrape.get_target_boundary_from_dest_db()
    scrape.scrape()

