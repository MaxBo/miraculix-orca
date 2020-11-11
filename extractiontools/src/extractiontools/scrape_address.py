#!/usr/bin/env python
#coding:utf-8

import numpy as np
import logging
import time
from argparse import ArgumentParser

import requests
from requests.exceptions import ConnectionError
import sys
import json

import os
import datetime
from extractiontools.ausschnitt import DBApp, Extract, Connection, BBox, logger


class ScrapeAdresses(Extract):
    schema = 'verwaltungsgrenzen'
    role = 'group_osm'
    table = 'adresses'

    def __init__(self,
                 options,
                 db='extract'):
        super(ScrapeAdresses, self).__init__(destination_db=db, options=options)
        self.db = db
        self.options = options

    def scrape(self):
        """"""
        with Connection(login=self.login) as conn1:
            self.conn1 = conn1
            self.conn = conn1
            self.read_adresses()
            self.conn1.commit()

    def get_cursor(self):
        """erzeuge Datenbankverbindung1 und Cursor"""
        cursor = self.conn1.cursor()
        cursor.execute('SET search_path TO {s}, public'.format(s=self.schema))
        return cursor

    def read_laea_raster(self, cursor):
        """read coords of laea-raster"""
        sql = """
SELECT
  st_xmin(geom) AS lon0,
  st_xmax(geom) AS lon1,
  st_ymin(geom) AS lat0,
  st_ymax(geom) AS lat1
FROM (SELECT st_transform(geom, 3035) AS geom
FROM laea.laea_vector_100) a;
        """
        cursor.execute(sql)
        rows = cursor.fetchall()
        return rows

    def read_adresses(self):
        """Lies Adressen und füge sie in DB ein bzw. aktualisiere sie"""

        cursor = self.get_cursor()
        #gridcells = self.read_laea_raster(cursor)
        table = self.table
        strasse = 'strassen'

        sql_create_table = """
CREATE TABLE IF NOT EXISTS {s}.{t} (
id text primary key,
geom geometry(POINT, {srid}),
bundesland text,
kreis text,
verwgem text,
rs text,
ags text,
gemeinde text,
plz text,
ort text,
ortsteil text,
strasse text,
haus text,
text text,
lat double precision,
lon double precision
)
;
""".format(s=self.schema, t=table, srid=self.target_srid)
        self.run_query(sql_create_table)

        sql_create_table = """
CREATE TABLE IF NOT EXISTS {s}.{t} (
id text primary key,
geom geometry(POINT, {srid}),
bundesland text,
kreis text,
verwgem text,
rs text,
ags text,
gemeinde text,
plz text,
ort text,
ortsteil text,
strasse text,
text text,
lat double precision,
lon double precision
)
;
""".format(s=self.schema, t=strasse, srid=self.target_srid)
        self.run_query(sql_create_table)

        sql_insert = """
INSERT INTO {s}.{t}
(id, bundesland, kreis, verwgem, rs, ags, gemeinde, plz, ort, ortsteil,
strasse, haus, text, lat, lon, geom)
SELECT
  %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
  st_transform(st_setsrid(st_makepoint( %s, %s), 4326 ),
  %s::integer)
WHERE NOT EXISTS (SELECT 1 FROM {s}.{t} a WHERE a.id = %s);
""".format(s=self.schema, t=table)

        sql_insert_strasse = """
INSERT INTO {s}.{t}
(id, bundesland, kreis, verwgem, rs, ags, gemeinde, plz, ort, ortsteil,
strasse, text, lat, lon, geom)
SELECT
  %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
  st_transform(st_setsrid(st_makepoint( %s, %s), 4326 ),
  %s::integer)
WHERE NOT EXISTS (SELECT 1 FROM {s}.{t} a WHERE a.id = %s);
""".format(s=self.schema, t=strasse)

        lon0, lon1, lat0, lat1 = self.bbox.rounded()
        adresses_found = 0
        adresses_inserted = 0
        step = self.options.step
        decoder = json.JSONDecoder()
        #for (lon0, lon1, lat0, lat1) in gridcells:
        lat0 = 53.60
        for i0 in np.arange(lat0, lat1, step):
            i1 = i0 + step
            for j0 in np.arange(lon0, lon1, step):
                j1 = j0 + step
                adresses_found_in_tile = 0
                adresses_inserted_in_tile = 0

                time.sleep(0.2)
                #logger.info('search in {}, {}'.format(i0, j0))

                bbox = '{j0},{i0},{j1},{i1}'.format(
                    i0=i0, i1=i1, j0=j0, j1=j1)

                params = {'bbox': bbox,
                          'filter': 'typ:haus',
                          #'filter': 'typ:strasse',
                          'count': self.options.max_count,}

                # URL zusammensetzen
                url = 'http://sg.geodatenzentrum.de/gdz_ortssuche__{u}/geosearch'.format(
                    u=self.options.uuid)
                success = False
                tries = 3
                while not success and tries:
                    try:
                        page = requests.get(url, params=params)
                    except ConnectionError as e:
                        time.sleep(30)
                        tries -= 1
                    else:
                        success = True
                jsonadresses = page.json()
                features = jsonadresses.get('features', [])
                print(len(features))
                for f in features:
                    lon, lat = f['geometry']['coordinates']
                    fid = f['id']
                    p = f['properties']

                    #Datenobjekt erzeugen und in DB schreiben

                    cursor.execute(
                        sql_insert,
                        #sql_insert_strasse,
                        (fid,
                         p['bundesland'],
                         p['kreis'],
                         p.get('verwgem', ''),
                         p['rs'],
                         p['ags'],
                         p.get('gemeinde', ''),
                         p['plz'],
                         p['ort'],
                         p['ortsteil'],
                         p['strasse'],
                         p['haus'],
                         p['text'],
                         lat,
                         lon,
                         lon,
                         lat,
                         self.target_srid,
                         fid))
                    adresses_found += 1
                    adresses_found_in_tile += 1
                    adresses_inserted += cursor.rowcount
                    adresses_inserted_in_tile += cursor.rowcount


                if not adresses_found % 1000:
                    self.conn1.commit()

                logger.info(' found {} new adresses'.format(
                    adresses_inserted_in_tile))
                self.conn1.commit()

        logger.info('{} adresses found and inserted'.format(
            adresses_inserted))


if __name__=='__main__':


    parser = ArgumentParser(
        description="Scrape Adresses in a given bounding box")

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
    parser.add_argument('--uuid', action="store",
                        help="uuid-code for bkg",
                        dest="uuid",
                        default='453fd547-0194-f207-3dc0-4dc91caab5c3')

    parser.add_argument('--step', action="store",
                        help="step in wgs84-coords",
                        dest="step",
                        type=float,
                        default=0.01)
    parser.add_argument('--maxcount', action="store",
                        help="maximum number of features per tile",
                        dest="max_count",
                        type=int,
                        default=1000)

    options = parser.parse_args()

    scrape = ScrapeAdresses(options, db=options.destination_db)
    scrape.set_login(host=options.host, port=options.port, user=options.user)
    scrape.get_target_boundary_from_dest_db()
    scrape.scrape()
