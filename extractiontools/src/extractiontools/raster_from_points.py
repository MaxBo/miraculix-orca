#!/usr/bin/env python
#coding:utf-8

import os
import sys
import tempfile
import subprocess
import logging
logger = logging.getLogger()
logger.addHandler(logging.StreamHandler())
logger.level = logging.DEBUG
from extractiontools.connection import Connection, DBApp, Login


class PixelType(object):
    """pixel types for PostGIS Raster Bands"""
    pixeltypes = {
'1BB': '1-bit boolean',
'2BUI': '2-bit unsigned integer',
'4BUI': '4-bit unsigned integer',
'8BSI': '8-bit signed integer',
'8BUI': '8-bit unsigned integer',
'16BSI': '16-bit signed integer',
'16BUI': '16-bit unsigned integer',
'32BSI': '32-bit signed integer',
'32BUI': '32-bit unsigned integer',
'32BF': '32-bit float',
'64BF': '64-bit float',
    }

    def __init__(self, pixeltype):
        """
        Parameters
        ----------
        pixeltype : str
        """
        try:
            self.description = self.pixeltypes[pixeltype]
            self.pixeltype = pixeltype
        except KeyError:
            allTypes = '\n '.join(iter(self.pixeltypes.keys()))
            msg = 'pixeltype {p} not defined. Please try one of these:\n{a}'
            raise ValueError(msg.format(p=pixeltype,
                                        a=allTypes))


class Points2Raster(DBApp):
    """
    Create the target DB and Extract the Meta Tables
    """
    schema = 'laea'
    role = 'group_osm'
    reference_raster = 'laea.laea_raster_100'
    reference_vector = 'laea.laea_vector_100'
    srid = 3035

    def __init__(self,
                 options,
                 db='extract'):
        self.db = db
        self.check_platform()
        self.options = options

    def set_login(self, host, port, user, password=None):
        self.login = Login(host, port, user, password, db=self.db)

    def check_platform(self):
        """
        check the platform
        """
        if sys.platform.startswith('win'):
            self.folder = r'C:\temp'
            self.SHELL = False
        else:
            self.folder = '$HOME/gis'
            self.SHELL = True

    def run(self):
        """
        """
        with Connection(login=self.login) as conn:
            # preparation
            self.conn = conn
            self.set_session_authorization(self.conn)
            self.do_stuff()
            self.conn.commit()
            self.reset_authorization(self.conn)

    def do_stuff(self):
        raise NotImplementedError('Do be defined in the subclass')

    def point2raster(self,
                     point_feature,
                     geom_col,
                     value_col,
                     target_raster,
                     pixeltype='32BF',
                     srid=3035,
                     reference_raster='laea.laea_raster_100',
                     raster_pkey='rid',
                     raster_col='rast',
                     band=1,
                     noData=0,
                     initial=0,
                     overwrite=False,
                     ):
        """
        converts a point feature to a raster feature

        Parameters
        ----------
        point_feature : str
            the [schema.]table name point feature with the values
        geom_col : str
            the column with the geometry
        value_col : str
            the column with the values to add
        target_raster
            the [schema.]table name of the table to create
        reference_raster : str
            the [schema.]table of the reference raster
        pixeltype : str, optional (default=float32)
            the pixeltype of the raster to create
        srid : int, optional (default = 3035)
            the sird of the target raster
        reference_raster : str
            the the [schema.]table of the refefence raster
        raster_pkey : str
            the column with the raster id
        raster_col : str
            the column with the raster values
        band : int
            the band to create
        noData : float, optional (default=0)
            the NoData-Value
        initial : double, optional (default=0)
            the initial value
        overwrite : bool
            default=False

        """
        # validate pixeltype
        pt = PixelType(pixeltype)
        # get schema-name if given
        target_schema_table = target_raster.split('.')
        if len(target_schema_table) == 2:
            target_schema = target_schema_table[0]
            schema_argument = "'{}'::name, ".format(target_schema)
        else:
            # no schema given
            schema_argument = ""
        # table-name
        target_raster_tablename = target_schema_table[-1]

        if overwrite:
            sql = """
DROP TABLE IF EXISTS {target} CASCADE;
""".format(target=target_raster)
            self.run_query(sql)

        sql = """
CREATE TABLE IF NOT EXISTS {target}
(
        {rid} serial NOT NULL,
        {rast} raster,
        CONSTRAINT {target_tn}_pkey PRIMARY KEY ({rid}),
        CONSTRAINT enforce_srid_rast CHECK (st_srid({rast}) = {srid})
        );

INSERT INTO {target} ({rid}, {rast})
SELECT
r.{rid},
st_setvalues(
  st_addband(
    ST_MakeEmptyRaster(r.{rast}),
    '{pixeltype}'::text,
    {initial}::double precision,
    {nd}::double precision),
    {band},
    rv.geomval_arr
) AS {rast}

FROM
{ref_raster} r,
(SELECT
  r.{rid},
  array_agg((v.{geom_col}, v.{value_col})::geomval) AS geomval_arr
FROM
  {ref_raster} r,
  {point_feature} v
WHERE
  v.{geom_col} && r.{rast}
GROUP BY
  r.{rid}
  ) AS rv
WHERE
  rv.{rid} = r.{rid};
        """.format(
            target=target_raster,
            schema=schema_argument,
            target_tn=target_raster_tablename,
            rid=raster_pkey,
            rast=raster_col,
            pixeltype=pt.pixeltype,
            srid=srid,
            band=band,
            nd=noData,
            initial=initial,
            ref_raster=reference_raster,
            point_feature=point_feature,
            geom_col=geom_col,
            value_col=value_col,
           )
        self.run_query(sql)
        self.add_raster_index(schema=target_schema,
                              tablename=target_raster_tablename,
                              raster_column=raster_col,
                              conn=self.conn)

    def export2tiff(self,
                    tablename,
                    subfolder='tiffs',
                    raster_col='rast'):
        """
        export the data to tiff files
        """
        tempfile_hex = tempfile.TemporaryFile
        tempfile_hex = '/tmp/{tn}.hex'.format(tn=tablename)

        folder = os.path.join(self.folder,
                              'projekte',
                              self.options.destination_db,
                              self.options.subfolder, )
        ret = subprocess.call('mkdir -p {}'.format(folder), shell=self.SHELL)

        fn = '{tn}.tiff'.format(tn=tablename)
        file_path = os.path.join(folder, fn)

        copy_sql = """
COPY (
  SELECT encode(
    ST_AsTIFF(st_union({rast}), 'LZW'),
    'hex') AS png
  FROM {s}.{tn})
  TO STDOUT;
    """.format(
           s=self.schema,
           tn=tablename,
           rast=raster_col,
       )

        with tempfile.NamedTemporaryFile('wb', delete=False) as f:
            cur = self.conn.cursor()
            logger.info(copy_sql)
            cur.copy_expert(copy_sql, f)
            f.close()

            cmd = 'xxd -p -r {tf} > {file_path}'.format(tf=f.name,
                                                        file_path=file_path)
            logger.info(cmd)
            ret = subprocess.call(cmd, shell=self.SHELL)
            try:
                os.remove(f.name)
            except IOError:
                pass
        if ret:
            msg = 'Raster Table {tn} could copied to {df}'
            raise IOError(msg.format(tn=tablename, df=file_path))

    def create_matview_poly_with_raster(self,
                                        tablename,
                                        source_table,
                                        value_column):
        """
        Create a materialized view for a polygon layer that intersects
        with a raster grid

        Creates a materialized view {tablename}
        with a value for each raster cellcode

        and a view {tablename}_pnt joining this with the raster centroids

        Parameters
        ----------
        tablename : str
            the name of the view to create
        source_table : str
            [schema.]tablename of the polygon layer that sould be intersected
        value_column : str
            column in source_table that should be distributed to the
            target raster
        """

        sql = """
DROP MATERIALIZED VIEW IF EXISTS {sc}.{tn} CASCADE;
CREATE MATERIALIZED VIEW {sc}.{tn} AS
SELECT
l.cellcode,
sum(st_area(st_intersection(g.geom, l.geom)) / st_area(g.geom) * g.{val}) as value
FROM {st} g,
{rv} l
WHERE l.geom && g.geom
GROUP BY l.cellcode;
CREATE INDEX {tn}_pkey ON
{sc}.{tn} USING btree(cellcode);

ANALYZE {sc}.{tn};

CREATE OR REPLACE VIEW {sc}.{tn}_pnt AS
SELECT
v.cellcode, v.value, l.pnt_laea,
row_number() OVER(ORDER BY v.cellcode)::integer AS rn
FROM
{sc}.{tn} v,
{rv} l
WHERE v.cellcode=l.cellcode;
        """
        self.run_query(sql.format(sc=self.schema,
                                  tn=tablename,
                                  st=source_table,
                                  rv=self.reference_vector,
                                  val=value_column))

    def create_matview_poly_weighted_with_raster(self,
                                                 tablename,
                                                 source_table,
                                                 id_column,
                                                 value_column,
                                                 weights, ):
        """
        Create a materialized view for a polygon layer that intersects
        with a raster grid

        Creates a materialized view {tablename}
        with a value for each raster cellcode

        and a view {tablename}_pnt joining this with the raster centroids

        Parameters
        ----------
        tablename : str
            the name of the view to create
        source_table : str
            [schema.]tablename of the polygon layer that sould be intersected
        value_column : str
            column in source_table that should be distributed to the
            target raster
        id_column : str
            column in source_table with the primary key
        weights : str
            the raster with the weights
        """

        sql = """

DROP MATERIALIZED VIEW IF EXISTS {sc}.{tn}_raster_intersects CASCADE;
CREATE MATERIALIZED VIEW {sc}.{tn}_raster_intersects AS
SELECT
l.cellcode,
g.{pkey} AS pkey,
g.{val}::double precision AS value,
(st_area(st_intersection(g.geom, l.geom)) / l.area * l.raster_weight) as weight
FROM {st} g,
(SELECT
l.cellcode,
l.geom,
st_area(l.geom) AS area,
(p).val AS raster_weight
FROM
{rv} l,
(SELECT st_pixelascentroids(r.rast) AS p
FROM
{wr} r) r
WHERE l.geom && (p).geom
AND (p).val IS NOT NULL
) AS l
WHERE l.geom && g.geom;

CREATE OR REPLACE VIEW {sc}.{tn} AS

SELECT
s.cellcode,
sum(s.val) AS value

FROM (
SELECT
i.cellcode,
CASE s.sum_weights
WHEN 0 THEN s.simple_weight * i.value
ELSE i.weight / s.sum_weights * i.value
END as val
FROM {sc}.{tn}_raster_intersects AS i,
(
-- add the weights for the zone
SELECT
  z.pkey,
  sum(z.weight) AS sum_weights,
  1 / count(*)::double precision AS simple_weight
FROM {sc}.{tn}_raster_intersects AS z
GROUP BY
  z.pkey
) s
WHERE s.pkey = i.pkey
) s
GROUP BY
s.cellcode
;

CREATE INDEX {tn}_pkey ON
{sc}.{tn}_raster_intersects USING btree(cellcode, pkey);

ANALYZE {sc}.{tn}_raster_intersects;

CREATE OR REPLACE VIEW {sc}.{tn}_pnt AS
SELECT
v.cellcode, v.value, l.pnt_laea,
row_number() OVER(ORDER BY v.cellcode)::integer AS rn
FROM
{sc}.{tn} v,
{rv} l
WHERE v.cellcode=l.cellcode;
        """
        self.run_query(sql.format(sc=self.schema,
                                  tn=tablename,
                                  st=source_table,
                                  rv=self.reference_vector,
                                  val=value_column,
                                  pkey=id_column,
                                  wr=weights))

    def create_matview_point_with_raster(self,
                                         tablename,
                                         source_table,
                                         value_column=None,
                                         geom='geom'):
        """
        Create a materialized view for a point raster intersected
        with the raster polygon
        """
        if value_column is None:
            val = 'count(*) as value'
        else:
            val = 'sum(g.{val}) as value'.format(val=value_column)

        sql = """
DROP MATERIALIZED VIEW IF EXISTS {sc}.{tn} CASCADE;
CREATE MATERIALIZED VIEW {sc}.{tn} AS
SELECT
l.cellcode,
{val}
FROM {st} g,
{rv} l
WHERE st_within(g.{geom}, l.geom)
GROUP BY l.cellcode;
CREATE INDEX {tn}_pkey ON
{sc}.{tn} USING btree(cellcode);

ANALYZE {sc}.{tn};
CREATE OR REPLACE VIEW {sc}.{tn}_pnt AS
SELECT
v.cellcode, v.value, l.pnt_laea,
row_number() OVER(ORDER BY v.cellcode)::integer AS rn
FROM
{sc}.{tn} v,
{rv} l
WHERE v.cellcode=l.cellcode;
        """
        self.run_query(sql.format(sc=self.schema,
                                  tn=tablename,
                                  st=source_table,
                                  rv=self.reference_vector,
                                  val=val,
                                  geom=geom))

    def create_raster_for_polygon(self,
                                  tablename,
                                  source_table,
                                  value_column,
                                  pixeltype='32BF',
                                  noData=0):
        """
        intersect polygon feature with raster and sum up the value_column
        """
        self.create_matview_poly_with_raster(
            tablename, source_table, value_column)

        self.create_raster_for_table(tablename, pixeltype, noData)

    def intersect_polygon_with_weighted_raster(self,
                                               tablename,
                                               id_column,
                                               source_table,
                                               value_column,
                                               weights,
                                               pixeltype='32BF',
                                               noData=0):
        """
        intersect polygon feature with raster,
        distribute the value of the value_column weighted according to the
        weights in the weights-raster
        """
        self.create_matview_poly_weighted_with_raster(
            tablename, source_table, id_column, value_column, weights)

        self.create_raster_for_table(tablename, pixeltype, noData)


    def create_raster_for_point(self,
                                tablename,
                                source_table,
                                value_column=None,
                                pixeltype='32BF',
                                noData=0,
                                geom='geom'):
        """
        intersect point feature with raster and create raster tiff

        Parameters
        ----------
        tablename : str
            the tablename to create in the destination schema
        source_table : str
            the tablename of the point shape
        value_column : str, optional
            if given, sum up the values in value_column for each rastercell,
            otherwise, count the points for each rastercell
        pixeltype : str, optional(Default='32BF')
            the pixeltype of the resulting raster
        noData : numeric, optional(Default=0)
            the no-data-value of the resulting raster
        geom : str, optional(Default='geom')
            the name of the geometry column
        """
        self.create_matview_point_with_raster(
            tablename, source_table, value_column, geom)

        self.create_raster_for_table(tablename, pixeltype, noData)

    def create_raster_for_table(self,
                                tablename,
                                pixeltype='32BF',
                                noData=0,
                                value_col='value'):
        """
        intersect feature with raster and create raster tiff
        """

        self.point2raster(
            point_feature='{sc}.{tn}_pnt'.format(sc=self.schema,
                                                 tn=tablename),
            geom_col='pnt_laea',
            value_col=value_col,
            target_raster='{sc}.{tn}_raster'.format(sc=self.schema,
                                                    tn=tablename),
            pixeltype=pixeltype,
            srid=self.srid,
            reference_raster=self.reference_raster,
            raster_pkey='rid',
            raster_col='rast',
            band=1,
            noData=noData,
            overwrite=True)


class Points2km2Raster(Points2Raster):
    """Convert PointDate to km2-Raster"""
    reference_raster = 'laea.laea_raster_1000'
    reference_vector = 'laea.laea_vector_1000'
