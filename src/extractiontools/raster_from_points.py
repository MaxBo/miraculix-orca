#!/usr/bin/env python
#coding:utf-8

import os
import sys
import subprocess
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
            allTypes = '\n '.join(self.pixeltypes.iterkeys())
            msg = 'pixeltype {p} not defined. Please try one of these:\n{a}'
            raise ValueError(msg.format(p=pixeltype,
                                        a=allTypes))


class Points2Raster(DBApp):
    """
    Create the target DB and Extract the Meta Tables
    """
    schema = 'laea'
    role = 'group_osm'


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
        overwrite : bool
            default=False

        """
        # validate pixeltype
        pt = PixelType(pixeltype)
        target_raster_tablename = target_raster.split('.')[-1]

        if overwrite:
            sql = """
DROP TABLE IF EXISTS {target};
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
    '{pixeltype}'::text),
      {band}, rv.geomval_arr
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
            target_tn=target_raster_tablename,
            rid=raster_pkey,
            rast=raster_col,
            pixeltype=pt.pixeltype,
            srid=srid,
            band=band,
            ref_raster=reference_raster,
            point_feature=point_feature,
            geom_col=geom_col,
            value_col=value_col,
           )
        self.run_query(sql)

    def export2tiff(self,
                    tablename,
                    subfolder='tiffs',
                    raster_col='rast'):
        """
        export the data to tiff files
        """
        tempfile_hex = '/tmp/{tn}.hex'.format(tn=tablename)

        folder = os.path.join(self.folder,
                              'projekte',
                              self.options.destination_db,
                              self.options.subfolder, )
        ret = subprocess.call('mkdir -p {}'.format(folder), shell=self.SHELL)

        fn = '{tn}.tiff'.format(tn=tablename)
        file_path = os.path.join(folder, fn)

        sql = """
COPY (
  SELECT encode(
    ST_AsTIFF({rast}, 'LZW'),
    'hex') AS png
  FROM {s}.{tn})
  TO '{tf}';
""".format(
       s=self.schema,
       tn=tablename,
       rast=raster_col,
       tf=tempfile_hex,
   )
        self.run_query(sql)


        cmd = 'xxd -p -r {tf} > {file_path}'.format(tf=tempfile_hex,
                                                    file_path=file_path)
        logger.info(full_cmd)
        ret = subprocess.call(full_cmd, shell=self.SHELL)
        if ret:
            msg = 'Raster Table {tn} could copied to {df}'
            raise IOError(msg.format(tn=tablename, df=file_path))


