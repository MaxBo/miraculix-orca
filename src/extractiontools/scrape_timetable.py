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

    def __init__(self, options,
                 db='extract'):
        """"""
        super(ScrapeTimetable, self).__init__(options, db=db)
        self.set_date(options.year, options.month, options.day)
        self.recreate_tables = options.recreate_tables

    def set_date(self, year, month, day):
        """
        """
        self.date = Date(year, month, day)

    def scrape(self):
        """"""
        with Connection(login=self.login1) as conn1:
            self.conn1 = conn1
            if self.recreate_tables:
                self.create_timetable_tables()
                self.conn1.commit()

            self.get_fahrten_for_stops()

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
  abfahrt_id_final integer,
  keep boolean,
  route_name_long text,
  kreis text,
  kennz text,
  typ text,
  route_short_name text,
  agency_name text,
  route_id integer,
  CONSTRAINT abfahrten_pkey PRIMARY KEY (abfahrt_id),
  CONSTRAINT abfahrten_fk FOREIGN KEY ("H_ID")
      REFERENCES haltestellen ("H_ID") MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE NO ACTION,
  CONSTRAINT abfahrten_fk1 FOREIGN KEY (abfahrt_id_final)
      REFERENCES abfahrten (abfahrt_id) MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE NO ACTION
);

TRUNCATE abfahrten CASCADE;

CREATE INDEX IF NOT EXISTS abfahrten_sh_idx
  ON abfahrten
  USING btree
  ("Fahrt_Name", "Fahrt_Ziel");

CREATE INDEX IF NOT EXISTS abfahrten_idx1
  ON abfahrten
  USING btree
  (keep)
  WHERE keep IS NULL OR keep;

CREATE TABLE IF NOT EXISTS fahrten
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
  CONSTRAINT fahrten_idx PRIMARY KEY (abfahrt_id, fahrt_index),
  CONSTRAINT fahrten_fk FOREIGN KEY (abfahrt_id)
      REFERENCES abfahrten (abfahrt_id) MATCH SIMPLE
      ON UPDATE CASCADE ON DELETE CASCADE
);

TRUNCATE fahrten;

CREATE INDEX IF NOT EXISTS fahrten_idx1
  ON fahrten
  USING btree
  ("H_Name", "H_Abfahrt");
        """
        cursor = self.get_cursor()
        cursor.execute(sql)
        self.conn1.commit()

    def get_fahrten_for_stops(self):
        """get the stops in the area"""
        sql = """
SELECT "H_ID", "H_Name"
FROM haltestellen;
        """
        cursor = self.get_cursor()
        cursor.execute(sql)
        rows = cursor.fetchall()
        i = 0
        for row in rows:
            H_ID = row[0]
            H_Name = row[1].decode('utf8')
            i += 1
            logger.info(u'{}: {}: {}'.format(i, H_ID, H_Name))
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
        abfahrt_id = rows[0][0] or 0

        #Abfahrten auslesen
        H_IDstr = str(H_ID_Abfahrtstafel)
        try:
            Kurz_URL = ''.join((
                'http://mobile.bahn.de/bin/mobil/bhftafel.exe/dox?si=',
                H_IDstr,
                '&bt=dep&ti=00:00%2B1&p=1111111111&date=',
                str(self.date),
                '&max=10000&rt=1&use_realtime_filter=1&start=yes&'))
        except:
            print 'fehler in kurz_URL'
            pass
        #logger.debug(Kurz_URL)
        Page = self.urlquery(Kurz_URL)
        Page = self.htmlentitydecode(Page)
        try:
            StundenDaten = Page.split('<div class="sqdetailsDep trow">')[1:]

            Stundenliste = []
            FahrtListe = []
            sql = """SELECT 1 as id
            FROM abfahrten AS a, fahrten AS f"""  +"""
            WHERE a.abfahrt_id = f.abfahrt_id
            AND a."Fahrt_Name" = %s AND f."H_Name" = %s
            AND f."H_Abfahrt" = %s AND a."Fahrt_Ziel" = %s """

            vorhandene_Fahrten = 0
            neue_Fahrten = 0
            for Fahrt in StundenDaten:

                Zeit = Fahrt.split('<span class="bold">')[2]
                Zeit = Zeit.split('</span>')[0]

                Stunde = Zeit.split(':')[0]
                if len(Stunde) ==2:
                    Fahrt_Name = Fahrt.split('<span class="bold">')[1].split(
                        '</span>\n')[0]
                    Fahrt_Ziel = Fahrt.split('<span class="bold">')[1].split(
                        '\n&gt;&gt;\n')[1].split('\n<br />\n')[0]
                    Fahrt_Abfahrt = time.strptime(Zeit, '%H:%M')
                    Stunde = int(Stunde)
                    FahrtParameter = (Fahrt_Name,
                                      H_Name_Abfahrtstafel,
                                      self.date.get_timestamp(Fahrt_Abfahrt),
                                      Fahrt_Ziel)
                    # schaue in der DB, ob Fahrt schon vorhanden
                    cursor.execute(sql, FahrtParameter)
                    rows = cursor.fetchall()
                    # wenn nicht, suche in dieser Stunde
                    if not rows:
                        Stundenliste.append(Stunde)
                        neue_Fahrten += 1
                    else:
                        vorhandene_Fahrten += 1
            logger.info('{in_db} Fahrten schon definitiv in DB, suche {new} potenziell neue Fahrten: '.format(in_db=vorhandene_Fahrten,
                                                                                                              new=neue_Fahrten))
            Stundenliste = set(Stundenliste)

            for stunde in Stundenliste:
                logger.info('{}{} h'.format(os.linesep, stunde))
                sleeptime = float(random.randint(1,4))
                time.sleep(sleeptime)

                # grab data from station timetable for specific hour
                bhftafel_URL = 'http://reiseauskunft.bahn.de/bin/bhftafel.exe/dn?ld=96242&country=DEU&rt=1&evaId=%s&bt=dep&time=%02d:00&date=%s&productsFilter=1111111111&max=10000&start=yes' %(
                    H_IDstr, stunde, self.date)
                tree = self.getRequestsTree(bhftafel_URL)
                subtree = tree.xpath('//*[@id="sqResult"]/table')

                MAX_TRIES = 10
                t = 0
                while t < MAX_TRIES and not subtree:
                    # try to get page 3 times
                    time.sleep(2)
                    tree = self.getRequestsTree(bhftafel_URL)
                    subtree = tree.xpath('//*[@id="sqResult"]/table')
                    t += 1
                    logger.info('try {} for url {}'.format(t, bhftafel_URL))
                try:
                    fahrten_count = len(subtree[0].findall("tr")) - 3 # remove 3 rows for header & footer
                except:
                    logger.warn('fehler bei erstellung von fahrten_count')
                    continue
                if fahrten_count:
                    for i in range(0, fahrten_count + 1):
                        try:
                            xpath_base = '//*[@id="journeyRow_'+str(i)+'"]'
                            Fahrt_Ziel = tree.xpath(xpath_base+'/td[4]/span/a/text()')[0].replace('\n','')
                            Abfahrten = tree.xpath(xpath_base+'/td[4]/text()')[2:][0].split('\n')
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
                            #logger.warn('fehler beim Auslesen der BHF-Tafel')
                            #exc_type, exc_obj, exc_tb = sys.exc_info()
                            #fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                            #print(exc_type, fname, exc_tb.tb_lineno)
                            #time.sleep(10)
                            continue

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

                                abfahrt_id += 1
                                #Fahrten in Tabelle Abfahrten schreiben
                                sql1 = """
INSERT INTO abfahrten
(abfahrt_id, "Fahrt_URL", "Fahrt_Name", "Fahrt_Abfahrt", "H_ID", "Fahrt_Ziel")
VALUES (%s, %s, %s, %s, %s, %s);"""
                                cursor.execute(sql1,
                                               (abfahrt_id,
                                                Fahrt_URL,
                                                Fahrt_Name,
                                                self.date.get_timestamp(Fahrt_Abfahrt),
                                                H_ID_Abfahrtstafel,
                                                Fahrt_Ziel))
                                logger.info(Fahrt_Name)
                                try:
                                    sleeptime = float(random.randint(2,5))/5
                                    time.sleep(sleeptime)
                                    Fahrtverlauf = self.urlquery(Fahrt_URL)
                                except:
                                    print 'fehler in Abfrage des Fahrtverlaufs'
                                    pass

                                try:
                                    VerlaufParser = MyHTMLParser()
                                    VerlaufParser.query_date = self.date
                                except:
                                    logger.warn('Fehler bei VerlaufParser')
                                    pass
                                # wenn Uhrzeit zwischen 0 und 4 h und Ankunft vor 4 Uhr ist, wird der Vortag als "Zuglauf" ausgegeben
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
                                                   (abfahrt_id,
                                                    Fahrt_Name,
                                                    h+1,
                                                    H_Name,
                                                    get_timestamp2(H_Ankunft),
                                                    get_timestamp2(H_Abfahrt),
                                                    akt_H_ID))
                                VerlaufParser.close()

                            else:
                                # wenn Fahrt schon vorhanden
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

        except Exception:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            logger.warn('{} {} {}'.format(
                exc_type, fname, exc_tb.tb_lineno))
            pass

# create a subclass and override the handler methods
class MyHTMLParser(HTMLParser):
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
                self.date= data.split('Fahrtverlauf vom: ')[1].rstrip(')')
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
                    if self.ist_Starthaltestelle:
                        if len(data.strip()) > 0:
                            # wenn Abfahrt an der Starthaltestelle vor 4 Uhr
                            if int(data.strip()[:2]) < 4:
                                # und Ankunft an der Starthaltestelle vor 4 Uhr
                                if self.Ankunft_vor_4Uhr:
                                    # korrigiere das Datum auf den Tag der Anfrage
                                    self.date = self.query_date #shift_day(self.date, 1)
                        self.ist_Starthaltestelle = False
                    zeit = time.strptime(data.strip() + ' %s' %self.date, '%H:%M %d.%m.%y')
                    if self.data_departures:
                        z = len(self.data_departures)-1
                        while z >= 0:
                            abfahrtszeit = self.data_departures[z]
                            if abfahrtszeit:
                                if zeit < abfahrtszeit:
                                    # setze auf Folgetag
                                    self.date = shift_day(self.date, 1)
                                    zeit = time.strptime(data.strip() + ' %s' %self.date, '%H:%M %d.%m.%y')
                                break
                            z -= 1
                except Exception as F:
                    print F
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
            else:
                try:
                    zeit = time.strptime(data.strip() + ' %s' %self.date, '%H:%M %d.%m.%y')
                    if self.data_arrivals[-1]:
                        if zeit < self.data_arrivals[-1]:
                            self.date = shift_day(self.date, 1)
                            zeit = time.strptime(data.strip() + ' %s' %self.date, '%H:%M %d.%m.%y')
                    else:
                        # wenn keine Ankunftszeit angegeben ist, schaue, ob Zeitsprung an Abfahrt an vorheriger Haltestelle
                        z = len(self.data_departures)-1
                        while z >= 0:
                            abfahrtszeit = self.data_departures[z]
                            if abfahrtszeit:
                                if zeit < abfahrtszeit:
                                    # setze auf Folgetag
                                    self.date = shift_day(self.date, 1)
                                    zeit = time.strptime(data.strip() + ' %s' %self.date, '%H:%M %d.%m.%y')
                                break
                            z -= 1
                except:
                    zeit = None
                self.data_departures.append(zeit)


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

    parser.add_argument('--day', action="store", type=int,
                        help="day, default: day of today",
                        dest="day")
    parser.add_argument('--month', action="store", type=int,
                        help="month, default: month of today",
                        dest="month")
    parser.add_argument('--year', action="store", type=int,
                        help="year, default: year of today",
                        dest="year")

    parser.add_argument('--recreate-tables', action="store_true",
                        help="recreate the tables", default=False,
                        dest="recreate_tables")

    options = parser.parse_args()

    scrape = ScrapeTimetable(options, options.destination_db)
    scrape.set_login(host=options.host, port=options.port, user=options.user)
    scrape.get_target_boundary_from_dest_db()
    scrape.scrape()

