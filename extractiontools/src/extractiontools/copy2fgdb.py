#!/usr/bin/env python
# coding:utf-8

from argparse import ArgumentParser
from typing import Dict

import sys
import os
import subprocess
from extractiontools.connection import Login, Connection
from extractiontools.ausschnitt import Extract


class Copy2FGDB(Extract):
    """Copy files to gdal"""

    gdal_file_extensions = {'FileGDB': 'gdb',
                            'GPKG': 'gpkg',
                            }

    def __init__(self,
                 destination_db,
                 layers: Dict[str, str],
                 filename: str,
                 schema: str = None,
                 logger=None
                 ):
        """"""
        super().__init__(destination_db, logger=logger)
        self.layers = layers
        self.filename = filename
        self.schema = schema

    def copy_layer(self,
                   schema: str,
                   layer: str,
                   dest_schema: str = None,
                   gdal_format: str = 'FileGDB'):
        """
        copy layer
        Parameters
        ----------
        layer : str
        """
        if gdal_format not in self.gdal_file_extensions:
            raise ValueError(f'{gdal_format} not implemented')

        ext = self.gdal_file_extensions[gdal_format]

        lco = ''
        if gdal_format == 'FileGDB':
            lco = f' -lco FEATURE_DATASET="{dest_schema}"'

        if self.filename is None:
            filename = f'{self.destination_db}.{ext}'
        else:
            filename = self.filename
        if not filename.endswith(f'.{ext}'):
            filename += f'.{ext}'

        # get srid
        if self.target_srid is None:
            srid = self.get_target_srid()
            srid_option = f'-a_srs EPSG:{srid}'
        else:
            srid_option = f'-t_srs EPSG:{self.target_srid}'

        folder = os.path.abspath(
            os.path.join(self.folder,
                         'projekte',
                         self.destination_db,
                         gdal_format, )
        )
        os.makedirs(folder, exist_ok=True)
        path = os.path.join(folder, filename)

        cmd = f'{self.OGR2OGRPATH} -overwrite -geomfield geom -nln {layer} '\
            f'{srid_option}{lco} -f "{gdal_format}" {path} '\
            f'PG:"host={self.login.host} port={self.login.port} user={self.login.user} '\
            f'dbname={self.destination_db}" "{schema}.{layer}"'

        self.logger.info(cmd)
        ret = subprocess.call(cmd, shell=self.SHELL)
        if ret:
            raise IOError(
                f'Layer {layer} could not be copied to {gdal_format}')

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

    def copy_layers(self, gdal_format: str = 'FileGDB'):
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
            self.copy_layer(schema, layer, dest_schema, gdal_format)

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
            self.OGR_FOLDER = '/usr/local/bin'
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
    copy2fgdb.copy_layers('FileGDB')
