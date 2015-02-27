#!/usr/bin/env python
#coding:utf-8

from argparse import ArgumentParser

import numpy as np
import sys
import os
import subprocess

from extractiontools.ausschnitt import Extract


class ExtractLAEA(Extract):
    """
    Create the target DB and Extract the Meta Tables
    """
    schema = 'landuse'

    def additional_stuff(self):
        """
        create_laea-raster in the destination db
        parameters: pixelsize in meters (default=100)
        search lower left corner and round value
        make empty raster in espg:3035
        add dummy band
        create raster-polygons and centroids with cellcode
        reproject them to the target-srid

        Verschneide mit Gewichtungsgrößen (Einwohner, ...)
        Verschneide mit Gemeindegrenzen
        """

        sql = """
CREATE TABLE landuse.laea_test (rid serial primary key, rast raster);
INSERT INTO landuse.laea_test (rast)
SELECT st_tile(st_addband(
st_makeemptyraster({width}, {hight}, {left}, {upper}, {pixelsize}, {pixelsize}, 0, 0, 3035)
, '2BUI', {default}),
{tilesize}, {tilesize});


CREATE TABLE landuse.laea_test2 (
  cellcode text primary key,
  geom Geometry(Polygon, 3035),
  pnt Geometry(Point, 3035));

INSERT INTO landuse.laea_test2
SELECT '{str_pixelsize}E' || floor(ST_x(geom)/{pixelsize})::text || 'N' || floor(st_y(geom)/{pixelsize})::text AS cellcode, poly AS geom
FROM (
  SELECT
  (ST_PixelAsPoints(rast, 1)).*,
  (st_pixelaspolygons(rast, 1)).geom AS poly,
  (st_pixelascentroids(rast, 1)).geom AS point FROM landuse.laea_test l) b
;
CreATE INDEX laea_test2_geom_idx ON landuse.laea_test2 USING gist(geom);
ANALYZE landuse.laea_test2;



        """
        #cursor.execute(sql.format(temp=self.temp))


if __name__ == '__main__':

    parser = ArgumentParser(description="Extract Data for Model")

    parser.add_argument("-n", '--name', action="store",
                        help="Name of destination database",
                        dest="destination_db", default='extract')

    parser.add_argument("-s", '--srid', action="store",
                        help="srid of the target database", type=int,
                        dest="srid", default='31467')

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
                        dest="host", default='gis.ggr-planung.de')
    parser.add_argument("-p", '--port', action="store",
                        help="port", type=int,
                        dest="port", default=5432)

    parser.add_argument("-U", '--user', action="store",
                        help="database user",
                        dest="user", default='max')
    parser.add_argument('--source-db', action="store",
                        help="source database",
                        dest="source_db", default='dplus')

    options = parser.parse_args()

    bbox = BBox(top=options.top, bottom=options.bottom,
                left=options.left, right=options.right)
    extract = ExtractOSM(source_db=options.source_db,
                         destination_db=options.destination_db,
                         target_srid=options.srid,
                         recreate_db=True)
    extract.set_login(host=options.host, port=options.port, user=options.user)
    extract.get_target_boundary(bbox)
    extract.extract()

    extract = ExtractLanduse(source_db=options.source_db,
                             destination_db=options.destination_db,
                             target_srid=options.srid,
                             recreate_db=False)
    extract.set_login(host=options.host, port=options.port, user=options.user)
    extract.get_target_boundary(bbox)
    extract.extract()
