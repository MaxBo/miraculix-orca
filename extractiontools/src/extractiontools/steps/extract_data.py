"""orca-steps for miraculix"""

from typing import List, Dict
import orca
from orcadjango.decorators import group
from extractiontools.extract_osm import ExtractOSM
from extractiontools.osm2polygons import CreatePolygons
from extractiontools.extract_landuse import ExtractLanduse
from extractiontools.extract_verwaltungsgrenzen import ExtractVerwaltungsgrenzen
from extractiontools.laea_raster import ExtractLAEA
from extractiontools.zensus2raster import Zensus2Raster
from extractiontools.copy_osm2fgdb import CopyOSM2FGDB

import extractiontools.steps.create_db
import extractiontools.steps.network

__parent_modules__ = [
    'extractiontools.steps.create_db',
    'extractiontools.steps.network',
]


@group('(2) Extract Data', order=1)
@orca.step()
def extract_osm(source_db: str, database: str, target_srid: str):
    """
    extract osm data for the bbox
    """
    extract = ExtractOSM(source_db=source_db, destination_db=database,
                         target_srid=target_srid, logger=orca.logger)
    extract.get_target_boundary_from_dest_db()
    extract.extract()


@group('(2) Extract Data', order=2)
@orca.step()
def create_polygons_from_osm(database: str):
    """
    extract osm data for the bbox
    """
    copy2fgdb = CreatePolygons(destination_db=database, logger=orca.logger)
    copy2fgdb.get_target_boundary_from_dest_db()
    copy2fgdb.create_poly_and_multipolygons()


@group('(2) Extract Data', order=3)
@orca.step()
def extract_landuse(source_db: str, database: str, gmes: List[str],
                    corine: List[str], target_srid: str):
    """
    extract landuse data for the bbox
    """
    extract = ExtractLanduse(source_db=source_db, destination_db=database,
                             gmes=gmes, corine=corine, target_srid=target_srid,
                             logger=orca.logger)
    extract.get_target_boundary_from_dest_db()
    extract.extract()


@group('(2) Extract Data', order=10)
@orca.step()
def extract_verwaltungsgrenzen(source_db: str, database: str,
                               verwaltungsgrenzen_tables: List[str],
                               target_srid: str):
    """
    extract administrative boundaries for the bbox
    """
    tables = {f: 'geom' for f in verwaltungsgrenzen_tables}
    extract = ExtractVerwaltungsgrenzen(source_db=source_db,
                                        destination_db=database, tables=tables,
                                        logger=orca.logger)
    extract.get_target_boundary_from_dest_db()
    extract.extract()


@group('(2) Extract Data', order=4)
@orca.step()
def extract_laea_raster(source_db: str, database: str, target_srid: str):
    """
    extract laea raster for the bbox
    """
    extract = ExtractLAEA(source_db=source_db, destination_db=database,
                          logger=orca.logger)
    extract.get_target_boundary_from_dest_db()
    extract.extract()


@group('(2) Extract Data', order=5)
@orca.step()
def zensus2raster(database: str, subfolder_tiffs: str):
    """
    create views for zensus data on raster grid
    """
    z2r = Zensus2Raster(destination_db=database, subfolder=subfolder_tiffs,
                        logger=orca.logger)
    z2r.run()


@group('Export')
@orca.injectable()
def osm_layers() -> Dict[str, str]:
    """the network layers to export to the corresponding schema in a FGDB"""
    layers = {'railways': 'osm_layer',
              'buildings': 'osm_layer',
              'leisure_pnt': 'osm_layer',
              'leisure_polys': 'osm_layer',
              'natural': 'landuse',
              'waterways_lines': 'landuse',
              'amenity_pnt': 'osm_layer',
              'amenity_polys': 'osm_layer',
              'tourism_pnt': 'osm_layer',
              'tourism_polys': 'osm_layer',
              }
    return layers


@group('Export')
@orca.step()
def copy_osm_to_fgdb(database: str,
                     osm_layers: Dict[str, str]):
    """
    create osm layers and copy osm stuff to a file-gdb
    attention: drops cascadingly the depending views
    """

    copy2fgdb = CopyOSM2FGDB(destination_db=database,
                             layers=osm_layers,
                             gdbname='osm_layers.gdb',
                             schema='osm_layer',
                             logger=orca.logger)
    copy2fgdb.create_views()
    copy2fgdb.copy_layers()


@group('Export')
@orca.step()
def copy_to_fgdb(database: str,
                 osm_layers: Dict[str, str]):
    """copy osm stuff to a file-gdb"""

    copy2fgdb = CopyOSM2FGDB(destination_db=database,
                             layers=osm_layers,
                             gdbname='osm_layers.gdb',
                             schema='osm_layer',
                             logger=orca.logger)
    copy2fgdb.copy_layers()
