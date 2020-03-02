"""orca-steps for miraculix"""

from typing import List, Dict
import orca
from orcadjango.decorators import group
from extractiontools.injectables.database import Login
from extractiontools.extract_osm import ExtractOSM
from extractiontools.osm2polygons import CreatePolygons
from extractiontools.extract_landuse import ExtractLanduse
from extractiontools.extract_verwaltungsgrenzen import ExtractVerwaltungsgrenzen
from extractiontools.laea_raster import ExtractLAEA
from extractiontools.zensus2raster import Zensus2Raster
from extractiontools.copy_osm2fgdb import CopyOSM2FGDB

import extractiontools.steps.create_db
import extractiontools.steps.network

__parent_modules__ = ['extractiontools.steps.create_db',
                      'extractiontools.steps.network',
                      ]


@group('ExtractData', order=1)
@orca.step()
def extract_osm(source_db: str, login: Login):
    """
    extract osm data for the bbox
    """
    extract = ExtractOSM(source_db=source_db,
                         destination_db=login.db)
    extract.set_login01(login, source_db)
    extract.get_target_boundary_from_dest_db()
    extract.extract()


@group('ExtractData', order=2)
@orca.step()
def create_polygons_from_osm(source_db: str, login: Login):
    """
    extract osm data for the bbox
    """
    copy2fgdb = CreatePolygons(login)
    copy2fgdb.get_target_boundary_from_dest_db()
    copy2fgdb.create_poly_and_multipolygons()


@group('ExtractData', order=3)
@orca.step()
def extract_landuse(source_db: str, login: Login,
                    gmes: List[str], corine: List[str]):
    """
    extract landuse data for the bbox
    """
    extract = ExtractLanduse(source_db=source_db,
                             destination_db=login.db,
                             gmes=gmes,
                             corine=corine)
    extract.set_login01(login, source_db)
    extract.get_target_boundary_from_dest_db()
    extract.extract()


@group('ExtractData', order=10)
@orca.step()
def extract_verwaltungsgrenzen(source_db: str, login: Login,
                               verwaltungsgrenzen_tables: List[str]):
    """
    extract administrative boundaries for the bbox
    """
    extract = ExtractVerwaltungsgrenzen(source_db=source_db,
                                        destination_db=login.db)
    extract.tables = {f: 'geom' for f in verwaltungsgrenzen_tables}
    extract.set_login01(login, source_db)
    extract.get_target_boundary_from_dest_db()
    extract.extract()


@group('ExtractData', order=4)
@orca.step()
def extract_laea_raster(source_db: str, login: Login):
    """
    extract laea raster for the bbox
    """
    extract = ExtractLAEA(source_db=source_db,
                          destination_db=login.db)
    extract.set_login01(login, source_db)
    extract.get_target_boundary_from_dest_db()
    extract.extract()


@group('ExtractData', order=5)
@orca.step()
def zensus2raster(login: Login, subfolder_tiffs: str):
    """
    create views for zensus data on raster grid
    """
    z2r = Zensus2Raster(db=login.db, subfolder=subfolder_tiffs)
    z2r.login = z2r.login1 = login
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
def copy_osm_to_fgdb(login: Login,
                     osm_layers: Dict[str, str]):
    """
    create osm layers and copy osm stuff to a file-gdb
    attention: drops cascadingly the depending views
    """

    copy2fgdb = CopyOSM2FGDB(login=login,
                             layers=osm_layers,
                             gdbname='osm_layers.gdb',
                             schema='osm_layer')
    copy2fgdb.create_views()
    copy2fgdb.copy_layers()


@group('Export')
@orca.step()
def copy_to_fgdb(login: Login,
                 osm_layers: Dict[str, str]):
    """copy osm stuff to a file-gdb"""

    copy2fgdb = CopyOSM2FGDB(login=login,
                             layers=osm_layers,
                             gdbname='osm_layers.gdb',
                             schema='osm_layer')
    copy2fgdb.copy_layers()
