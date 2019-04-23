from typing import List
import orca
from extractiontools.injectables.database import Login
from extractiontools.extract_osm import ExtractOSM, Extract
from extractiontools.osm2polygons import CreatePolygons
from extractiontools.extract_landuse import ExtractLanduse
from extractiontools.extract_verwaltungsgrenzen import ExtractVerwaltungsgrenzen
from extractiontools.laea_raster import ExtractLAEA
from extractiontools.zensus2raster import Zensus2Raster

import extractiontools.steps.create_db
import extractiontools.steps.network

__parent_modules__ = ['extractiontools.steps.create_db',
                      'extractiontools.steps.network',
                      ]


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


@orca.step()
def create_polygons_from_osm(source_db: str, login: Login):
    """
    extract osm data for the bbox
    """
    copy2fgdb = CreatePolygons(login)
    copy2fgdb.get_target_boundary_from_dest_db()
    copy2fgdb.create_poly_and_multipolygons()


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

@orca.step()
def zensus2raster(login: Login, subfolder_tiffs: str):
    """
    create views for zensus data on raster grid
    """
    z2r = Zensus2Raster(db=login.db, subfolder=subfolder_tiffs)
    z2r.login1 = login
    z2r.run()





