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
    schema = 'laea'

    def final_stuff(self):
        """Final steps in the destiantion db"""

        self.add_raster_constraint(pixelsize=100)
        self.add_raster_constraint(pixelsize=1000)
        self.create_views_zensus()


    def create_views_zensus(self):
        """
        create views for zensus data and
        add a geometry index to views
        """
        sql = """
    CREATE MATERIALIZED VIEW
    {schema}.ew_hectar AS
    SELECT
    row_number() OVER(ORDER BY v.cellcode)::integer AS id,
    v.geom,
    v.pnt,
    z.einwohner
    FROM
    {schema}.zensus_ew_hectar z,
    {schema}.laea_vector_100 v
    WHERE v.cellcode = z.id;

    CREATE INDEX ew_hectar_geom_idx ON {schema}.ew_hectar USING gist(geom);
        """.format(schema=self.schema)
        self.run_query(sql, conn=self.conn1)

    def add_raster_constraint(self, pixelsize):
        sql = """


SELECT AddRasterConstraints('{schema}'::name,
                            'laea_raster_{pixelsize}'::name,
                            'rast'::name,
                            True);
        """.format(schema=self.schema,
                   pixelsize=pixelsize)
        self.run_query(sql, conn=self.conn1)


    def additional_stuff(self):
        """
        """

        self.create_raster(100)
        self.create_raster(1000)

        self.get_einwohner_hectar()
        #self.get_zensusdata_km2()

    def get_einwohner_hectar(self):
        """
        add a table with the number of residents per hectar
        """
        sql = """
DROP TABLE IF EXISTS {schema}.zensus_ew_hectar CASCADE;
CREATE TABLE {schema}.zensus_ew_hectar
(id text primary key, einwohner integer);

INSERT INTO {schema}.zensus_ew_hectar(id, einwohner)
SELECT z.id, z.einwohner
FROM zensus.ew_zensus2011_gitter z,
{schema}.laea_vector_100 v
WHERE v.cellcode = z.id;

        """.format(schema=self.temp)
        self.run_query(sql, conn=self.conn0)


    def create_raster(self, pixelsize):
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

DROP TABLE IF EXISTS {schema}.laea_raster_{pixelsize};
CREATE TABLE {schema}.laea_raster_{pixelsize}
(rid serial primary key, rast raster);

WITH b AS (SELECT
  st_xmin(a.geom) AS left,
  st_ymax(a.geom) AS upper,
  ceil((st_xmax(a.geom) - st_xmin(a.geom)) / {pixelsize})::integer AS width,
  ceil((st_ymax(a.geom) - st_ymin(a.geom)) / {pixelsize})::integer AS hight
FROM
(SELECT st_transform(geom, 3035) AS geom from {schema}.boundary) a)

INSERT INTO {schema}.laea_raster_{pixelsize} (rast)
SELECT
st_tile(
  st_addband(
    st_setsrid(
      st_makeemptyraster(b.width,
                         b.hight,
                         b.left,
                         b.upper,
                         {pixelsize})
     , 3035)
    , '1BB'::text, {default}),
  {tilesize}, {tilesize})

FROM b;

""".format(pixelsize=pixelsize,
           tilesize=self.get_tilesize(pixelsize),
           default=1,
           schema=self.temp)

        self.run_query(sql, conn=self.conn0)

        sql = """

DROP TABLE IF EXISTS {schema}.laea_vector_{pixelsize};
CREATE TABLE {schema}.laea_vector_{pixelsize} (
  cellcode text primary key,
  geom Geometry(Polygon, {srid}),
  pnt Geometry(Point, {srid}),
  pnt_laea Geometry(Point, 3035)
  );

INSERT INTO {schema}.laea_vector_{pixelsize}
SELECT
'{str_pixelsize}N' ||
floor(ST_y(geom)/{pixelsize})::text ||
'E' || floor(st_x(geom)/{pixelsize})::text AS cellcode,
poly AS geom,
pnt,
pnt_laea
FROM (
  SELECT
  (ST_PixelAsPoints(rast, 1)).*,
  st_transform((st_pixelaspolygons(rast, 1)).geom, {srid}) AS poly,
  st_transform((st_pixelascentroids(rast, 1)).geom, {srid}) AS pnt,
  (st_pixelascentroids(rast, 1)).geom AS pnt_laea
  FROM {schema}.laea_raster_{pixelsize} l) b
;
CREATE INDEX laea_vector_{pixelsize}_geom_idx
ON {schema}.laea_vector_{pixelsize} USING gist(geom);
ANALYZE {schema}.laea_vector_{pixelsize};

        """.format(pixelsize=pixelsize,
                   str_pixelsize=self.str_pixelsize(pixelsize),
                   srid=self.target_srid,
                   schema=self.temp,
                   )

        self.run_query(sql, conn=self.conn0)

    def get_tilesize(self, pixelsize):
        """
        return an according tilesize for the raster data

        Parameters
        ----------
        pixelsize: int

        Returns
        -------
        tilesize: int
        """
        # return 50*50 tiles (5km*5km)
        return 50

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
            return '%skm' % (size // 1000)
        else:
            return '%sm' % size


if __name__ == '__main__':

    parser = ArgumentParser(description="Create Raster Data")

    parser.add_argument("-n", '--name', action="store",
                        help="Name of destination database",
                        dest="destination_db", default='extract')

    parser.add_argument('--host', action="store",
                        help="host",
                        dest="host", default='localhost')
    parser.add_argument("-p", '--port', action="store",
                        help="port", type=int,
                        dest="port", default=5432)
    parser.add_argument("-U", '--user', action="store",
                        help="user", type=str,
                        dest="user", default='osm')

    parser.add_argument("--pixelsize", action="store",
                        help="pixelsize", type=int,
                        dest="pixelsize", default=100)

    parser.add_argument('--source-db', action="store",
                        help="source database",
                        dest="source_db", default='dplus')


    options = parser.parse_args()

    extract = ExtractLAEA(source_db=options.source_db,
                          destination_db=options.destination_db,
                          target_srid=None,
                          recreate_db=False)
    extract.set_login(host=options.host, port=options.port, user=options.user)
    extract.get_target_boundary_from_dest_db()
    extract.extract()
