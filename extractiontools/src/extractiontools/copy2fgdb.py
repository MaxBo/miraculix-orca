#!/usr/bin/env python
#coding:utf-8

from argparse import ArgumentParser
from typing import Dict

import sys
import os
import subprocess
from extractiontools.connection import Login, Connection
from extractiontools.ausschnitt import Extract


class Copy2FGDB(Extract):
    def __init__(self,
                 destination_db,
                 layers: Dict[str, str],
                 gdbname: str,
                 schema: str=None,
                 logger=None
                 ):

        """"""
        super().__init__(destination_db, logger=logger)
        self.layers = layers
        self.gdbname = gdbname
        self.schema = schema

    def copy_layer(self, schema, layer, dest_schema):
        """
        copy layer
        Parameters
        ----------
        layer : str
        """

        cmd = '{OGR2OGR} -overwrite -geomfield geom -nln {layer} {srid_option} -lco FEATURE_DATASET="{dest_schema}" -f "FileGDB" {path} PG:"host={host} port={port} user={user} dbname={db}" "{schema}.{layer}"'

        if self.gdbname is None:
            gdbname = '{db}.gdb'.format(db=self.destination_db)
        else:
            gdbname = self.gdbname
        if not gdbname.endswith('.gdb'):
            gdbname += '.gdb'

        # get srid
        if self.target_srid is None:
            srid = self.get_target_srid()
            srid_option = '-a_srs EPSG:{srid}'.format(srid=srid)
        else:
            srid_option = '-t_srs EPSG:{srid}'.format(
                srid=self.target_srid)

        folder = os.path.join(self.folder,
                              'projekte',
                              self.destination_db,
                              'fgdb', )
        self.make_folder(folder)
        path = os.path.join(folder, gdbname)

        full_cmd = cmd.format(OGR2OGR=self.OGR2OGRPATH,
                              layer=layer,
                              srid_option=srid_option,
                              path=path,
                              host=self.foreign_login.host,
                              port=self.foreign_login.port,
                              user=self.foreign_login.user,
                              db=self.foreign_login.db,
                              schema=schema,
                              dest_schema=dest_schema,
                              )
        self.logger.info(full_cmd)
        ret = subprocess.call(full_cmd, shell=self.SHELL)
        if ret:
            raise IOError('Layer {layer} could not be copied to FGDB'.format(layer=layer))

    def check_if_features(self, layer):
        """
        copy layer
        Parameters
        ----------
        layer : str
        """
        with Connection(self.login) as conn:
            cur = conn.cursor()
            sql = '''
SELECT * FROM {schema}.{layer} LIMIT 1;
            '''.format(schema=self.schema,
                       layer=layer)
            cur.execute(sql)
            return cur.rowcount

    def copy_layers(self):
        """
        copy all layers in option.layers
        """
        for full_layer, dest_schema in self.layers.items():
            self.dest_schema = dest_schema
            schema_layer = full_layer.split('.')
            if len(schema_layer) == 1:
                schema = self.schema
                layer = schema_layer[0]
            else:
                schema, layer = schema_layer
            self.copy_layer(schema, layer, dest_schema)

    def check_platform(self):
        """
        check the platform
        """
        super().check_platform()
        if sys.platform.startswith('win'):
            self.OGR_FOLDER = r'C:\Program Files\QGIS Brighton\bin'
            self.OGR2OGRPATH = os.path.join(self.OGR_FOLDER, 'ogr2ogr.exe')
            self.OGRINFO = os.path.join(self.OGR_FOLDER, 'ogrinfo.exe')
        else:
            self.OGR_FOLDER = '/opt/gdal-2/bin'
            self.OGR2OGRPATH = os.path.join(self.OGR_FOLDER, 'ogr2ogr')
            self.OGRINFO = os.path.join(self.OGR_FOLDER, 'ogrinfo')


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
    login = Login(options.host,
                  options.port,
                  options.user,
                  options.password,
                  options.destination_db)

    layers = dict()
    for layer in options.layers:
        layers[f'{options.schema}.{layer}'] = options.dest_schema

    copy2fgdb = Copy2FGDB(login, layers, options.gdbname)
    copy2fgdb.copy_layers()
