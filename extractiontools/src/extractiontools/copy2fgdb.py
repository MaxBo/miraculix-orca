#!/usr/bin/env python
#coding:utf-8

from argparse import ArgumentParser

import logging
logger = logging.getLogger()
#logger.addHandler(logging.StreamHandler())
logger.level = logging.DEBUG
import sys
import os
import subprocess
import psycopg2
from extractiontools.connection import Login, Connection
from extractiontools.ausschnitt import Extract

class Copy2FGDB(Extract):
    def __init__(self, options):

        """"""
        self.options = options
        self.check_platform()
        self.login1 = Login(self.options.host,
                            self.options.port,
                            self.options.user,
                            db=self.options.destination_db)

    def copy_layer(self, layer):
        """
        copy layer
        Parameters
        ----------
        layer : str
        """

        cmd = '{OGR2OGR} -overwrite -geomfield geom -nln {layer} {srid_option} -lco FEATURE_DATASET="{dest_schema}" -f "FileGDB" {path} PG:"host={host} port={port} user={user} dbname={db}" "{schema}.{layer}"'

        if self.options.gdbname is None:
            gdbname = '{db}.gdb'.format(db=self.options.destination_db)
        else:
            gdbname = self.options.gdbname
        if not gdbname.endswith('.gdb'):
            gdbname += '.gdb'

        # get srid
        if self.options.target_srid is None:
            srid = self.get_target_srid_from_dest_db()
            srid_option = '-a_srs EPSG:{srid}'.format(srid=srid)
        else:
            srid_option = '-t_srs EPSG:{srid}'.format(
                srid=self.options.target_srid)

        folder = os.path.join(self.folder,
                              'projekte',
                              self.options.destination_db,
                              'fgdb', )
        ret = subprocess.call('mkdir -p {}'.format(folder),
                              shell=self.SHELL)
        path = os.path.join(folder, gdbname)

        full_cmd = cmd.format(OGR2OGR=self.OGR2OGRPATH,
                              layer=layer,
                              srid_option=srid_option,
                              path=path,
                              host=self.options.host,
                              port=self.options.port,
                              user=self.options.user,
                              db=self.options.destination_db,
                              schema=self.options.schema,
                              dest_schema=self.options.dest_schema,
                              )
        logger.info(full_cmd)
        ret = subprocess.call(full_cmd, shell=self.SHELL)
        if ret:
            raise IOError('Layer {layer} could copied to FGDB'.format(layer=layer))

    def check_if_features(self, layer):
        """
        copy layer
        Parameters
        ----------
        layer : str
        """
        with Connection(self.login1) as conn:
            cur = conn.cursor()
            sql = '''
SELECT * FROM {schema}.{layer} LIMIT 1;
            '''.format(schema=self.options.schema,
                       layer=layer)
            cur.execute(sql)
            return cur.rowcount

    def copy_layers(self):
        """
        copy all layers in option.layers
        """
        for layer in self.options.layers:
            self.copy_layer(layer)
            #has_features = self.check_if_features(layer)
            #if has_features:
                #self.copy_layer(layer)
            #else:
                #logger.info('layer %s has no rows, will not be copied' % layer)

    def check_platform(self):
        """
        check the platform
        """
        if sys.platform.startswith('win'):
            self.OGR_FOLDER = r'C:\Program Files\QGIS Brighton\bin'
            self.OGR2OGRPATH = os.path.join(self.OGR_FOLDER, 'ogr2ogr.exe')
            self.OGRINFO = os.path.join(self.OGR_FOLDER, 'ogrinfo.exe')
            self.folder = r'C:\temp'
            self.SHELL = False
        else:
            self.OGR_FOLDER = '/opt/gdal-2/bin'
            self.OGR2OGRPATH = os.path.join(self.OGR_FOLDER, 'ogr2ogr')
            self.OGRINFO = os.path.join(self.OGR_FOLDER, 'ogrinfo')
            self.folder = '$HOME/gis'
            self.SHELL = True


if __name__ == '__main__':

    parser = ArgumentParser(description="Copy Data to File Geodatabase")

    parser.add_argument("-n", '--name', action="store",
                        help="Name of destination database",
                        dest="destination_db", default='extract')

    parser.add_argument("-s", '--srid', action="store",
                        help="srid of the target database", type=int,
                        dest="target_srid")

    parser.add_argument('--host', action="store",
                        help="host",
                        dest="host", default='gis.ggr-planung.de')
    parser.add_argument("-p", '--port', action="store",
                        help="port", type=int,
                        dest="port", default=5432)

    parser.add_argument("-U", '--user', action="store",
                        help="database user",
                        dest="user", default='osm')
    parser.add_argument('--schema', action="store",
                        help="schema",
                        dest="schema", default='network_car')
    parser.add_argument('--destschema', action="store",
                        help="destination schema in the FileGDB",
                        dest="dest_schema", default='network_car')
    parser.add_argument('--gdbname', action="store",
                        help="Name of the FileGDB to create",
                        dest="gdbname")
    parser.add_argument('--layers', action='store',
                        help='layers to copy,',
                        dest='layers',
                        nargs='+',
                        default=['autobahn',
                                 'hauptstr',
                                 'nebennetz',
                                 'faehren',
                                 'unaccessible_links',
                                 ])

    options = parser.parse_args()

    copy2fgdb = Copy2FGDB(options)
    copy2fgdb.copy_layers()