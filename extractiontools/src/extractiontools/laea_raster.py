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
    foreign_schema = 'zensus'
    schema = 'laea'

    def final_stuff(self):
        """Final steps in the destiantion db"""

        self.add_laea_raster_constraint(pixelsize=100)
        self.add_laea_raster_constraint(pixelsize=1000)
        self.create_views_zensus()

    def create_views_zensus(self):
        """
        create views for zensus data and
        add a geometry index to views
        """
        sql = f"""
        CREATE MATERIALIZED VIEW
        {self.schema}.ew_hectar AS
        SELECT
        row_number() OVER(ORDER BY v.cellcode)::integer AS id,
        v.geom,
        v.pnt,
        z.einwohner
        FROM
        {self.schema}.zensus_ew_hectar z,
        {self.schema}.laea_vector_100 v
        WHERE v.cellcode = z.id;

        CREATE INDEX ew_hectar_geom_idx ON {self.schema}.ew_hectar USING gist(geom);
        """
        self.run_query(sql, conn=self.conn)

    def add_laea_raster_constraint(self, pixelsize):
        tn = f'laea_raster_{pixelsize}'
        self.add_raster_index(schema=self.schema, tablename=tn)

    def additional_stuff(self):
        """
        """
        self.create_raster(100)
        self.create_raster(1000)
        self.create_grid_points_and_poly(100)
        self.create_grid_points_and_poly(1000)

        self.get_einwohner_hectar()
        self.get_zensusdata_km2()
        self.get_geostat_data_km2()

    def get_einwohner_hectar(self):
        """
        add a table with the number of residents per hectar
        """
        sql = f"""
        DROP TABLE IF EXISTS {self.schema}.zensus_ew_hectar CASCADE;
        CREATE TABLE {self.schema}.zensus_ew_hectar
        (id text primary key, einwohner integer);

        INSERT INTO {self.schema}.zensus_ew_hectar(id, einwohner)
        SELECT z.id, z.einwohner
        FROM {self.temp}.ew_zensus2011_gitter z,
        {self.schema}.laea_vector_100 v
        WHERE v.cellcode = z.id;
        """
        self.run_query(sql, conn=self.conn)


    def get_zensusdata_km2(self):
        """Extract Censusdata on km2 level for area"""
        sql = f"""
        DROP TABLE IF EXISTS {self.schema}.zensus_km2 CASCADE;
        CREATE TABLE {self.schema}.zensus_km2
        ( id text primary key,
          einwohner integer,
          alter_d double precision,
          unter18_a double precision,
          ab65_a double precision,
          auslaender_a double precision,
          hhgroesse_d double precision,
          leerstandsquote double precision,
          wohnfl_bew_d double precision,
          wohnfl_wohnung double precision);

        INSERT INTO {self.schema}.zensus_km2(
          id,
          einwohner,
          alter_d,
          unter18_a,
          ab65_a,
          auslaender_a,
          hhgroesse_d,
          leerstandsquote,
          wohnfl_bew_d,
          wohnfl_wohnung)
        SELECT z.id,
          z.einwohner,
          z.alter_d,
          z.unter18_a,
          z.ab65_a,
          z.auslaender_a,
          z.hhgroesse_d,
          z.leerstandsquote,
          z.wohnfl_bew_d,
          z.wohnfl_wohnung
        FROM {self.temp}.zensus2011_gitter1000m_spitze z,
        {self.schema}.laea_vector_1000 v
        WHERE v.cellcode = z.id;
        """
        self.run_query(sql, conn=self.conn)

    def get_geostat_data_km2(self):
        """Extract Geostat data on km2 level for area"""
        sql = f"""
        DROP TABLE IF EXISTS {self.schema}.geostat_km2 CASCADE;
        CREATE TABLE {self.schema}.geostat_km2
        ( id text primary key,
          einwohner integer);

        INSERT INTO {self.schema}.geostat_km2(
          id,
          einwohner)
        SELECT z.grid_id AS id,
          z.tot_p AS einwohner
        FROM {self.temp}.geostat_2011_pop_1km2 z,
        {self.schema}.laea_vector_1000 v
        WHERE v.cellcode = z.grid_id;
        """
        self.run_query(sql, conn=self.conn)

    def create_raster(self, pixelsize, boundary_name=None):
        """
        create_laea-raster in the destination db
        parameters: pixelsize in meters (default=100)
        search lower left corner and round value
        make empty raster in epsg:3035
        add dummy band
        create raster-polygons and centroids with cellcode
        reproject them to the target-srid

        Verschneide mit Gewichtungsgrößen (Einwohner, ...)
        Verschneide mit Gemeindegrenzen
        """

        tilesize = self.get_tilesize(pixelsize)
        default = 1
        sql = f"""
        DROP TABLE IF EXISTS {self.schema}.laea_raster_{pixelsize} CASCADE;
        CREATE TABLE {self.schema}.laea_raster_{pixelsize}
        (rid serial primary key, rast raster);

        WITH b AS (SELECT
          floor(st_xmin(a.geom) / {pixelsize}) * {pixelsize} AS left,
          ceil(st_ymax(a.geom) / {pixelsize}) * {pixelsize} AS upper,
          ceil((st_xmax(a.geom) - floor(st_xmin(a.geom) / {pixelsize}) * {pixelsize}) / {pixelsize})::integer AS width,
          ceil((ceil(st_ymax(a.geom) / {pixelsize}) * {pixelsize} - st_ymin(a.geom)) / {pixelsize})::integer AS hight
        FROM
        (SELECT st_transform(geom, 3035) AS geom from meta.boundary
         WHERE name='{boundary_name or self.boundary_name}') a)

        INSERT INTO {self.schema}.laea_raster_{pixelsize} (rast)
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
        """

        self.run_query(sql, conn=self.conn)
        str_pixelsize = self.str_pixelsize(pixelsize)
        srid = self.target_srid
        sql = f"""
        DROP TABLE IF EXISTS {self.schema}.laea_vector_{pixelsize} CASCADE;
        CREATE TABLE {self.schema}.laea_vector_{pixelsize} (
          cellcode text primary key,
          geom Geometry(Polygon, {srid}),
          pnt Geometry(Point, {srid}),
          pnt_laea Geometry(Point, 3035)
          );

        INSERT INTO {self.schema}.laea_vector_{pixelsize}
        SELECT
        '{str_pixelsize}N' ||
        (ST_y(geom)/{pixelsize} - 1)::text ||
        'E' || (st_x(geom)/{pixelsize})::text AS cellcode,
        poly AS geom,
        pnt,
        pnt_laea
        FROM (
          SELECT
          (ST_PixelAsPoints(rast, 1)).*,
          st_transform((st_pixelaspolygons(rast, 1)).geom, {srid}) AS poly,
          st_transform((st_pixelascentroids(rast, 1)).geom, {srid}) AS pnt,
          (st_pixelascentroids(rast, 1)).geom AS pnt_laea
          FROM {self.schema}.laea_raster_{pixelsize} l) b
        ;
        CREATE INDEX laea_vector_{pixelsize}_geom_idx
        ON {self.schema}.laea_vector_{pixelsize} USING gist(geom);
        CREATE INDEX laea_vector_{pixelsize}_pnt_idx
        ON {self.schema}.laea_vector_{pixelsize} USING gist(pnt_laea);
        ANALYZE {self.schema}.laea_vector_{pixelsize};

        """

        self.run_query(sql, conn=self.conn)

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

    def create_grid_points_and_poly(self, pixelsize):
        """
        Create views of points and polygons for the given pixelsize

        Parameters
        ----------
        pixelsize : int
        """
        sql = f"""
        CREATE OR REPLACE VIEW {self.schema}.grid_points_{pixelsize} AS
        SELECT
          cellcode,
          pnt AS geom
        FROM {self.schema}.laea_vector_{pixelsize};

        CREATE OR REPLACE VIEW {self.schema}.grid_poly_{pixelsize} AS
        SELECT
          cellcode,
          geom
        FROM {self.schema}.laea_vector_{pixelsize};
        """
        self.run_query(sql, conn=self.conn)


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
                        dest="source_db", default='europe')


    options = parser.parse_args()

    extract = ExtractLAEA(source_db=options.source_db,
                          destination_db=options.destination_db,
                          target_srid=None)
    extract.set_login(host=options.host, port=options.port, user=options.user)
    extract.get_target_boundary_from_dest_db()
    extract.extract()
