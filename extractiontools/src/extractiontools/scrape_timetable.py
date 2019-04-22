#!/usr/bin/env python
#coding:utf-8

from argparse import ArgumentParser

import os
import sys
import random
import time
import traceback
from html.parser import HTMLParser
import requests
from lxml import html
from extractiontools.utils.htmlentity import htmlentitydecode
from extractiontools.scrape_stops import ScrapeStops, Connection, logger
from extractiontools.utils.get_date import Date, get_timestamp2


class ScrapeTimetable(ScrapeStops):

    def __init__(self,
                 db: str,
                 date: str,
                 source_db: str,
                 recreate_tables: bool=True):
        """"""
        self.destination_db = db
        self.date: Date = Date.from_string(date)
        self.recreate_tables = recreate_tables
        self.source_db = source_db

    def scrape(self):
        """scrape timetables"""
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
  "Fahrt_Nr" text,
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
        n_total = len(rows)
        i = 0
        for row in rows:
            h_id = row[0]
            h_name = row[1]
            i += 1
            logger.info(f'{i}/{n_total}: {h_id}: {h_name}')
            self.read_fahrten(
                h_id_abfahrtstafel=h_id,
                h_name_abfahrtstafel=h_name)
            if not i % 1:
                self.conn1.commit()
        self.conn1.commit()

    def read_fahrten(self,
                    h_id_abfahrtstafel,
                    h_name_abfahrtstafel):
        """Lies Fahrten"""
        cursor = self.get_cursor()

        sql = """SELECT max(abfahrt_id) FROM abfahrten"""
        cursor.execute(sql)
        rows = cursor.fetchall()
        self.abfahrt_id = rows[0][0] or 0
        fahrten_count = 0

        #Abfahrten auslesen
        h_id_str = str(h_id_abfahrtstafel)

        # grab data from station timetable for the whole day
        # use 24:00 instead of 00:00 to really get all departures also on stations
        # with a lot of departures
        bhftafel_id_url = (
            f'http://reiseauskunft.bahn.de/bin/bhftafel.exe/dn?'
            f'ld=96242&country=DEU&rt=1&evaId={h_id_str}&'
            f'bt=dep&time=24:00&maxJourneys=10000&'
            f'date={self.date}&productsFilter=1111111111&max=10000&start=yes'
        )

        bhftafel_name_url = (
            f'http://reiseauskunft.bahn.de/bin/bhftafel.exe/dn?'
            f'ld=96242&country=DEU&rt=1&input={h_name_abfahrtstafel}&'
            f'bt=dep&time=24:00&maxJourneys=10000&date={self.date}&'
            f'productsFilter=1111111111&max=10000&start=yes'
            )
        urls = [bhftafel_id_url, bhftafel_name_url]

        try:
            # wait a bit
            sleeptime = random.randint(2, 4)
            time.sleep(sleeptime)
            # try first time to get the Bahnhofstafel
            MAX_TRIES = 10

            # try first the URL with the ID and if not successfully,
            # try the URL with the stop name
            for url in urls:
                self.t = 0
                r = requests.get(url)
                tree = html.fromstring(r.content)
                self.errCode = tree.xpath('//div[@class="hafasContent error"]/text()')

                while self.t < MAX_TRIES and self.errCode:
                    tree = self.wait_and_retry(url)
                try:
                    subtree = tree.xpath('//*[@id="sqResult"]/table')
                    if not subtree:
                        logger.warning(f'Fehler beim Lesen der BhfTafel für '
                                       f'{h_id_str}: {h_name_abfahrtstafel}:')
                        logger.warning(url)
                        # try next URL
                        continue
                    # count number of trips
                    elements = subtree[0].findall("tr")
                    # only elements with a attribute "key"
                    journey_rows = [e for e in elements if e.attrib.has_key('id')]
                    fahrten_count = len(journey_rows)

                except:
                    logger.warning(traceback.format_exc())
                    err_code = tree.xpath('//div[@class="errormsg leftMargin"]/text()')
                    logger.warning(f'Fehler beim Lesen der BhfTafel für '
                                   f'{h_id_str}: {h_name_abfahrtstafel}:')
                    if err_code:
                        logger.warning(err_code[0])
                    # try next URL
                    continue
                # found a Bahnhofstafel
                if fahrten_count:
                    logger.info(f'{fahrten_count} abfahrten in '
                                f'{h_id_str}: {h_name_abfahrtstafel}')
                    self.n_new = 0
                    self.n_already_in_db = 0
                    self.last_stunde = '-1'
                    journeys = (j.attrib['id'] for j in journey_rows)
                    for journey in journeys:
                        self.parse_fahrten(journey,
                                           fahrten_count,
                                           tree,
                                           h_id_abfahrtstafel,
                                           cursor,
                                           h_name_abfahrtstafel)
                    if self.n_new:
                        logger.info('')
                    logger.info(f'{self.n_new} new, '
                                f'{self.n_already_in_db} already in db')
                    # return and don't try next URL
                    return
        except Exception as e:
            logger.warning(traceback.format_exc())
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            logger.warning(f'{exc_type} {fname} {exc_tb.tb_lineno}')
            raise e

    def wait_and_retry(self, bhftafel_url: str, waittime: int=15):
        # if not successful,
        # try to get the page MAX_TRIES times
        logger.info(f'wait {waittime} secs and try again {self.t} '
                    f'for url {bhftafel_url}')
        time.sleep(waittime)
        r = requests.get(bhftafel_url)
        tree = html.fromstring(r.content)

        self.errCode = tree.xpath('//div[@class="hafasContent error"]/text()')
        self.t += 1
        return tree

    def parse_fahrten(self,
                      journey,
                      fahrten_count,
                      tree,
                      h_id_abfahrtstafel,
                      cursor,
                      h_name_abfahrtstafel):
        """
        Parse Fahrt journey in the tree and query the Fahrtverlauf
        look in Fahrten, if Fahrt already exists
        """
        try:
            xpath_base = f'//*[@id="{journey}"]'
            try:
                fahrt_ziel = tree.xpath(xpath_base+'/td[4]/span/a/text()')[0].replace('\n','')
                abfahrten = tree.xpath(xpath_base+'/td[4]/text()')[2:][0].split('\n')
            except IndexError:
                logger.warning(traceback.format_exc())
                logger.warning(f'Fehler beim parsen der Abfahrtstafel von {h_name_abfahrtstafel}')
            abfahrtshaltestelle = abfahrten[1].strip()
            abfahrtsuhrzeit = abfahrten[2]
            ankunftsuhrzeit = abfahrten[-2]
            fahrt_abfahrt = time.strptime(abfahrtsuhrzeit, '%H:%M')
            xpath_fahrt_base = xpath_base+'//td[3]/a'
            # search trip name, which can be marked as <span>
            name_span = tree.xpath(xpath_fahrt_base+'/span/text()')
            name_without_span = tree.xpath(xpath_fahrt_base+'/text()')
            if name_span:
                #  in this case, the second row is the trip number
                fahrt_name = name_span[0]
                fahrt_nr = name_without_span[-1].strip('\n()')
            else:
                fahrt_name = name_without_span[0].strip('\n')
                fahrt_nr = None

            fahrt_url = 'http://reiseauskunft.bahn.de/'+tree.xpath(xpath_base+'//td[3]/a/@href')[0]

            if fahrt_url:
                hst_id_abfahrt = int(fahrt_url.split('station_evaId=')[1].split('&')[0])
            else:
                hst_id_abfahrt = h_id_abfahrtstafel

        except:
            logger.warning(traceback.format_exc())
            # if there is an error, wait
            logger.warning('fehler beim Auslesen der BHF-Tafel')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            logger.warning(exc_type, fname, exc_tb.tb_lineno)
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
                           (fahrt_name,
                            abfahrtshaltestelle,
                            self.date.get_timestamp(fahrt_abfahrt),
                            fahrt_ziel))
            rows = cursor.fetchall()
            if not rows:
                self.n_new += 1
                self.abfahrt_id += 1

                stunde = abfahrtsuhrzeit[:2]
                if stunde != self.last_stunde:
                    self.last_stunde = stunde
                    print(os.linesep, stunde, end=',')

                #Fahrten in Tabelle Abfahrten schreiben
                sql1 = """
INSERT INTO abfahrten
(abfahrt_id, "Fahrt_URL", "Fahrt_Name", "Fahrt_Abfahrt", "H_ID", "Fahrt_Ziel", "Fahrt_Nr")
VALUES (%s, %s, %s, %s, %s, %s, %s);"""
                cursor.execute(sql1,
                               (self.abfahrt_id,
                                fahrt_url,
                                fahrt_name,
                                self.date.get_timestamp(fahrt_abfahrt),
                                h_id_abfahrtstafel,
                                fahrt_ziel,
                                fahrt_nr))
                print(f'{fahrt_name}', end=',')
                try:
                    sleeptime = float(random.randint(1,3))/20.
                    time.sleep(sleeptime)
                    r = requests.get(fahrt_url)
                    fahrtverlauf = htmlentitydecode(r.content)

                except:
                    logger.warning(traceback.format_exc())
                    logger.warning('Fehler in Abfrage des Fahrtverlaufs')
                    pass

                try:
                    verlauf_parser = MyHTMLParser()
                    verlauf_parser.query_date = self.date
                except:
                    logger.warning(traceback.format_exc())
                    logger.warning('Fehler bei VerlaufParser')
                    pass
                # wenn Uhrzeit zwischen 0 und 4 h und Ankunft vor 4 Uhr ist,
                # wird der Vortag als "Zuglauf" ausgegeben
                # dies muss korrigiert werden
                verlauf_parser.Ankunft_vor_4Uhr = int(ankunftsuhrzeit[:2]) < 4
                verlauf_parser.ist_Starthaltestelle = True

                #try:
                    #html_verlauf = html.fromstring(Fahrtverlauf.content)
                #except Exception as e:
                    #logger.warning(traceback.format_exc())
                    #logger.warning('HTML Verlauf Fehler')
                    #pass

                verlauf_parser.feed(fahrtverlauf)
                for h in range(len(verlauf_parser.data_stations)):
                    h_name = verlauf_parser.data_stations[h]

                    h_ankunft = verlauf_parser.data_arrivals[h]
                    h_abfahrt = verlauf_parser.data_departures[h]

                    if h_name == h_name_abfahrtstafel:
                        akt_h_id = h_id_abfahrtstafel
                    else:
                        akt_h_id = None


                    sql3 = """
INSERT INTO fahrten
(abfahrt_id, "Fahrt_Name", fahrt_index, "H_Name", "H_Ankunft", "H_Abfahrt", "H_ID")
VALUES (%s, %s, %s,%s,%s,%s,%s);"""
                    cursor.execute(sql3,
                                   (self.abfahrt_id,
                                    fahrt_name,
                                    h+1,
                                    h_name,
                                    get_timestamp2(h_ankunft),
                                    get_timestamp2(h_abfahrt),
                                    akt_h_id))
                verlauf_parser.close()

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
                               (h_id_abfahrtstafel,
                                fahrt_name,
                                h_name_abfahrtstafel,
                                self.date.get_timestamp(fahrt_abfahrt),
                                fahrt_ziel))
        except Exception as e:
            logger.warning(traceback.format_exc())
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            logger.warning('{} {} {}'.format(
                exc_type, fname, exc_tb.tb_lineno))
            raise e

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
                           sd=self.source_db,
                           s=self.schema)
        logger.info(query)
        cur.execute(query)
        logger.info(f'{cur.statusmessage}')


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
        self.date = Date()

    def handle_starttag(self, tag, attrs):

        if tag == 'td':
            if attrs:

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
            if attrs and attrs[0][1] == 'trainroute':
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
            if data != '\n':
                self.data_route.append(data)
        if self.recording_trainroute:
            if data != '\n':
                date = data.split('Fahrtverlauf vom ')[1].rstrip(')').replace('\n','')
                d, m, y = date.split('.')
                yyyy = int(y) + 2000
                self.date = Date(yyyy, m, d)
        elif self.recording_train:
            if data != '\n':
                self.data_train.append(data)
        elif self.recording_station:
            if data != '\n':
                self.data_stations.append(data)
        elif self.recording_arrival:
            if data in ['\n', '\n\xa0\n']: # geschütztes Leerzeichen bei Ankunft
                zeit = None
                self.data_arrivals.append(zeit)
                if self.first_linefeed:
                    self.first_linefeed = False
                else:
                    self.first_linefeed = True
            else:
                data = data.replace('an ','').replace('ab ','')
                try:
                    # if tag is Delay marker
                    if data.startswith('+'):
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
                    logger.warning(traceback.format_exc())
                    logger.warning(F)
                    zeit = None
                    raise
                self.data_arrivals.append(zeit)
        elif self.recording_departure:
            if data in ['\n', '\n\xa0\n']: # geschütztes Leerzeichen bei Abfahrt
                zeit = None
                self.data_departures.append(zeit)
                if self.first_linefeed:
                    self.first_linefeed = False
                else:
                    self.first_linefeed = True

            # Delay Marker
            elif data.startswith('+'):
                return
            # normal time
            else:
                data = data.replace('an ', '').replace('ab ', '')
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
                        # wenn keine Ankunftszeit angegeben ist, schaue,
                        # ob Zeitsprung an Abfahrt an vorheriger Haltestelle
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
                    logger.warning(traceback.format_exc())
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
            zeit = time.strptime(f'{data.strip()} {self.date}',
                                 '%H:%M %d.%m.%y')
        except ValueError as e:
            logger.warning(traceback.format_exc())
            logger.warning(
                f'"{data}" could not be processed by time.strptime')
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

    scrape = ScrapeTimetable(db=options.destination_db,
                             date=options.date,
                             source_db=options.source_db,
                             recreate_tables=options.recreate_tables)
    scrape.set_login(host=options.host, port=options.port, user=options.user)
    scrape.get_target_boundary_from_dest_db()
    scrape.scrape()

