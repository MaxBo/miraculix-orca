#!/usr/bin/env python
#coding:utf-8

from argparse import ArgumentParser

import logging
logger = logging.getLogger()
logger.addHandler(logging.StreamHandler())
logger.level = logging.DEBUG
import sys
import os
import subprocess


class Copy2FGDB(object):
    def __init__(self, options):

        """"""
        self.options = options
        self.check_platform()

    def copy_layer(self, layer):
        """
        copy layer
        Parameters
        ----------
        layer : str
        """
        cmd = '{OGR2OGR} -overwrite -geomfield geom -nln {layer} -a_srs EPSG:{srid} -lco FEATURE_DATASET="{dest_schema}" -f "FileGDB" {path} PG:"host={host} port={port} user={user} dbname={db}" "{schema}.{layer}"'

        gdbname = '{db}.gdb'.format(db=self.options.destination_db)
        path = os.path.join(self.folder, gdbname)

        full_cmd = cmd.format(OGR2OGR=self.OGR2OGRPATH,
                              layer=layer,
                              srid=self.options.srid,
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

    def copy_layers(self):
        """
        copy all layers in option.layers
        """
        for layer in self.options.layers:
            self.copy_layer(layer)


    def check_platform(self):
        """
        check the platform
        """
        if sys.platform.startswith('win'):
            self.OGR2OGRPATH = r'C:\Program Files\QGIS Brighton\bin\ogr2ogr.exe'
            self.folder = r'C:\temp'
            self.SHELL = False
        else:
            self.OGR2OGRPATH = '/usr/bin/ogr2ogr'
            self.folder = '/home/mb/gis'
            self.SHELL = True

if __name__ == '__main__':

    parser = ArgumentParser(description="Copy Data to File Geodatabase")

    parser.add_argument("-n", '--name', action="store",
                        help="Name of destination database",
                        dest="destination_db", default='extract')

    parser.add_argument("-s", '--srid', action="store",
                        help="srid of the target database", type=int,
                        dest="srid", default=31467)

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
