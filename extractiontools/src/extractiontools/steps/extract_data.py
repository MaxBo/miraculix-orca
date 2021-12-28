"""orca-steps for miraculix"""

from typing import List, Dict
import orca
from orcadjango.decorators import meta
from extractiontools.extract_osm import ExtractOSM
from extractiontools.osm2polygons import CreatePolygons
from extractiontools.extract_landuse import ExtractLanduse
from extractiontools.extract_verwaltungsgrenzen import (
    ExtractVerwaltungsgrenzen, ExtractFirmsNeighbourhoods)
from extractiontools.laea_raster import ExtractLAEA
from extractiontools.zensus2raster import Zensus2Raster, ExportZensus
from extractiontools.copy_osm2fgdb import CopyOSM2FGDB
import ogr

from extractiontools.pendlerdaten import ImportPendlerdaten

import extractiontools.steps.create_db
import extractiontools.steps.network
import extractiontools.steps.google

__parent_modules__ = [
    'extractiontools.steps.create_db',
    'extractiontools.steps.network',
]


@meta(group='(2) Extract Data', order=1, required='create_db')
@orca.step()
def extract_osm(source_db: str, database: str, target_srid: int,
                project_area: ogr.Geometry):
    """
    extract OSM data in the area
    """
    extract = ExtractOSM(source_db=source_db, destination_db=database,
                         target_srid=target_srid, logger=orca.logger,
                         boundary=project_area)
    extract.extract()


@meta(group='(2) Extract Data', order=2, required=extract_osm)
@orca.step()
def create_polygons_from_osm(database: str):
    """
    create polygons and multipolygons out of the OSM data
    """
    copy2fgdb = CreatePolygons(destination_db=database, logger=orca.logger)
    copy2fgdb.create_poly_and_multipolygons()


@meta(group='(2) Extract Data', order=4, required='create_db')
@orca.step()
def extract_landuse(source_db: str, database: str, gmes: List[str],
                    corine: List[str], target_srid: int,
                    project_area: ogr.Geometry):
    """
    extract landuse data in the area
    """
    extract = ExtractLanduse(source_db=source_db, destination_db=database,
                             gmes=gmes, corine=corine, target_srid=target_srid,
                             logger=orca.logger, boundary=project_area)
    extract.extract()


@meta(group='(2) Extract Data', order=10, required='create_db')
@orca.step()
def extract_verwaltungsgrenzen(source_db: str, database: str,
                               verwaltungsgrenzen_tables: List[str],
                               target_srid: int, project_area: ogr.Geometry):
    """
    extract administrative boundaries in the area
    """
    tables = {f: 'geom' for f in verwaltungsgrenzen_tables}
    extract = ExtractVerwaltungsgrenzen(source_db=source_db,
                                        destination_db=database, tables=tables,
                                        logger=orca.logger,
                                        boundary=project_area)
    extract.extract()


@meta(group='(2) Extract Data', order=11, required='create_db')
@orca.step()
def extract_firms_neighbourhoods(source_db: str, database: str,
                                 firms_tables: List[str],
                                 target_srid: int, project_area: ogr.Geometry):
    """
    extract firms and neighbourhoods in the area
    """
    tables = {f: 'geom' for f in firms_tables}
    extract = ExtractFirmsNeighbourhoods(source_db=source_db,
                                         destination_db=database,
                                         tables=tables,
                                         logger=orca.logger,
                                         boundary=project_area)
    extract.extract()


@meta(group='(2) Extract Data', order=5, required='create_db')
@orca.step()
def extract_laea_raster(source_db: str, database: str, target_srid: int,
                        project_area: ogr.Geometry):
    """
    extract laea and zensus raster in the area
    """
    extract = ExtractLAEA(source_db=source_db, destination_db=database,
                          logger=orca.logger, boundary=project_area)
    extract.extract()
    z2r = Zensus2Raster(db=database, logger=orca.logger)
    z2r.run()


@meta(group='(2) Extract Data', order=3, required=create_polygons_from_osm)
@orca.step()
def create_osm_views(database: str):
    """
    creates views on the OSM data;
    attention: drops already existing OSM views and dependent views cascadingly
    """
    copy2fgdb = CopyOSM2FGDB(destination_db=database,
                             layers=osm_layers,
                             filename='osm_layers.gdb',
                             schema='osm_layer',
                             logger=orca.logger)
    copy2fgdb.create_views()


@meta(group='(5) Export')
@orca.injectable()
def osm_layers() -> Dict[str, str]:
    """the OSM network layers to export to the corresponding schema"""
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


@meta(group='(5) Export', required=create_osm_views)
@orca.step()
def copy_osm_to_fgdb(database: str, osm_layers: Dict[str, str]):
    """
    copy OSM layers and views to a file-gdb
    """

    copy2fgdb = CopyOSM2FGDB(destination_db=database,
                             layers=osm_layers,
                             filename='osm_layers',
                             schema='osm_layer',
                             logger=orca.logger)
    copy2fgdb.copy_layers('FileGDB')


@meta(group='(5) Export', required=create_osm_views)
@orca.step()
def copy_osm_to_gpkg(database: str, osm_layers: Dict[str, str]):
    """
    copy OSM layers and views to a Geopackage
    """

    copy2fgdb = CopyOSM2FGDB(destination_db=database,
                             layers=osm_layers,
                             filename='osm_layers',
                             schema='osm_layer',
                             logger=orca.logger)
    copy2fgdb.copy_layers('GPKG')


@meta(group='(5) Export', required=extract_laea_raster)
@orca.step()
def copy_zensus_to_tiff(database: str, subfolder_tiffs: str):
    """
    export zensus to raster-TIFF files
    """
    z2r = ExportZensus(db=database, subfolder=subfolder_tiffs,
                       logger=orca.logger)
    z2r.run()


@meta(group='(8) Pendler', required=extract_verwaltungsgrenzen)
@orca.step()
def import_pendlerdaten(database: str,
                        subfolder_pendlerdaten: str,
                        pendlerdaten_years: List[int]):
    """
    import commutertrips to base database
    """
    import_pendler = ImportPendlerdaten(db=database,
                                        subfolder=subfolder_pendlerdaten,
                                        pendlerdaten_years=pendlerdaten_years,
                                        logger=orca.logger)
    import_pendler.run()
