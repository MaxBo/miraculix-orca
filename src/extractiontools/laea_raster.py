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
CREATE TABLE landuse.laea_raster (rid serial primary key, rast raster);
INSERT INTO landuse.laea_raster (rast)
SELECT st_tile(st_addband(
st_makeemptyraster({width}, {hight}, {left}, {upper}, {pixelsize}, {pixelsize}, 0, 0, 3035)
, '2BUI', {default}),
{tilesize}, {tilesize});


CREATE TABLE landuse.laea_vector (
  cellcode text primary key,
  geom Geometry(Polygon, 3035),
  pnt Geometry(Point, 3035));

INSERT INTO landuse.laea_vector
SELECT '{str_pixelsize}E' || floor(ST_x(geom)/{pixelsize})::text || 'N' || floor(st_y(geom)/{pixelsize})::text AS cellcode,
poly AS geom,
point AS pnt
FROM (
  SELECT
  (ST_PixelAsPoints(rast, 1)).*,
  (st_pixelaspolygons(rast, 1)).geom AS poly,
  (st_pixelascentroids(rast, 1)).geom AS point FROM landuse.laea_raster l) b
;
CREATE INDEX laea_vector_geom_idx ON landuse.laea_vector USING gist(geom);
ANALYZE landuse.laea_vector;



        """.format(tilesize=self.options.tilesize,
                   width=self.options.width,
                   height=self.options.height,
                   left=self.options.left,
                   upper=self.options.upper,
                   pixelsize=self.options.pixelsize,
                   str_pixelsize=self.str_pixelsize(self.options.pixelsize),
                   default=self.options.default_raster_value,
                   )
        cursor.execute(sql.format(temp=self.temp))

    def str_pixelsize(self, size):
        """
        return a representation of a pixelsize

        Parameters
        ----------
        size : int
            the pixelsize in m

        Returns
        -------
        str_pixelsize
            the string including m or km
        """
        available_sizes = [100, 250, 500, 1000, 2000, 5000, 10000]
        if size not in available_sizes:
            msg = 'size %s not in available sizes %s'
            raise ValueError(msg %(size, available_sizes))
        if size >= 1000:
            return '%skm' % size // 1000
        else:
            return '%sm' % size


if __name__ == '__main__':

    parser = ArgumentParser(description="Extract Data for Model")

    parser.add_argument("-n", '--name', action="store",
                        help="Name of destination database",
                        dest="destination_db", default='extract')

    parser.add_argument('--host', action="store",
                        help="host",
                        dest="host", default='gis.ggr-planung.de')
    parser.add_argument("-p", '--port', action="store",
                        help="port", type=int,
                        dest="port", default=5432)


    extract = ExtractLAEA(destination_db=options.destination_db,


                          recreate_db=False)
    extract.set_login(host=options.host, port=options.port, user=options.user)
    extract.get_target_boundary(bbox)
    extract.extract()
