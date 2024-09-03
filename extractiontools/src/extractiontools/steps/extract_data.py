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
from extractiontools.pendlerdaten import (ImportPendlerdaten,
                                          ExtractPendler,
                                          ExtractRegionalstatistik,
                                          CreatePendlerSpinne,
                                          ExportPendlerdaten)
from extractiontools.verschneidungstool import PrepareVerschneidungstool
from extractiontools.extract_bast_trafficdata import ExtractBASt
from osgeo import ogr

import extractiontools.steps.create_db
import extractiontools.steps.network
import extractiontools.steps.google

__parent_modules__ = [
    'extractiontools.steps.create_db',
    'extractiontools.steps.network',
]


@meta(group='(2) Datenextraktion', order=1, required='create_db',
      title='OSM-Daten extrahieren', description='OSM-Daten aus der '
      'Quelldatenbank extrahieren')
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


@meta(group='(2) Datenextraktion', order=2, required=extract_osm,
      title='Polygone erzeugen', description='(Multi-)Polygone aus den '
      'OSM-Daten erzeugen')
@orca.step()
def create_polygons_from_osm(database: str):
    """
    create polygons and multipolygons out of the OSM data
    """
    copy2fgdb = CreatePolygons(destination_db=database, logger=orca.logger)
    copy2fgdb.create_poly_and_multipolygons()


@meta(group='(2) Datenextraktion', order=4, required='create_db',
      title='Landnutzung extrahieren',
      description='Daten zur Landnutzung im Projektgebiet extrahieren')
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


@meta(group='(2) Datenextraktion', order=10, required='create_db',
      title='Verwaltungsgrenzen extrahieren',
      description='administrative Grenzen innerhalb des Projektgebiets '
      'extrahieren')
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


@meta(group='(2) Datenextraktion', order=11, required='create_db',
      title='Firmen und Nachbarschaften extrahieren',
      description='Firmen und Nachbarschaften innerhalb des '
      'Projektgebiets extrahieren')
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


@meta(group='(2) Datenextraktion', order=5, required='create_db',
      title='Raster extrahieren',
      description='LAEA- und Zensus-Raster innerhalb des '
      'Projektgebiets extrahieren')
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


@meta(group='(2) Datenextraktion', order=3, required=create_polygons_from_osm,
      title='OSM-Views erzeugen',
      description='erzeugt spezialisierte Views auf die OSM-Daten. <br>'
      '<b>Achtung</b>: bereits existierende OSM-Views und davon abhängende '
      'Views werden kaskadiert gelöscht!')
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


@meta(group='(5) Export', title='OSM Layer', description='Die Ziel-Schemata in '
      'der FGDB, in die die Tabellen mit den OSM-Daten exportiert werden.')
@orca.injectable()
def osm_layers() -> Dict[str, str]:
    """the OSM network layers to export to the corresponding schema"""
    layers = {'railways': 'osm_layer',
              'buildings': 'osm_layer',
              'leisure_pnt': 'osm_layer',
              'leisure_polys': 'osm_layer',
              'osm_landuse': 'landuse',
              'natural': 'landuse',
              'waterways_lines': 'landuse',
              'amenity_pnt': 'osm_layer',
              'amenity_polys': 'osm_layer',
              'tourism_pnt': 'osm_layer',
              'tourism_polys': 'osm_layer',
              }
    return layers


@meta(group='(5) Export', required=create_osm_views, title='OSM nach FGDB',
      description='Export der OSM-Layer in eine FGDB-Datei')
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


@meta(group='(5) Export', required=create_osm_views, title='OSM nach GPKG',
      description='Export der OSM-Layer in eine Geopackage-Datei')
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


@meta(group='(5) Export', required=extract_laea_raster, title='Zensus-TIFF',
      description='Export des Zensus in eine GeoTIFF')
@orca.step()
def copy_zensus_to_tiff(database: str, subfolder_tiffs: str):
    """
    export zensus to raster-TIFF files
    """
    z2r = ExportZensus(db=database, subfolder=subfolder_tiffs,
                       logger=orca.logger)
    z2r.run()


@meta(group='(8a) Regionalstatistik', title='Regionalstatistik extrahieren',
      description='Daten der Regionalstatistik der gegebenen Jahren in den '
      'gegebenen Gemeindegrenzen aus der Quelldatenbank extrahieren',
      required='create_db')
@orca.step()
def extract_regionalstatistik(source_db: str,
                              database: str,
                              regionalstatistik_gemeinden: str,
                              regionalstatistik_years: List[int]):
    """
    Regionalstatistik (SvB, Kfz, Arbeitslose) auf Gemeindeebene
    """
    extract_regionalstatistik = ExtractRegionalstatistik(
        source_db=source_db,
        destination_db=database,
        regionalstatistik_gemeinden=regionalstatistik_gemeinden,
        regionalstatistik_years=regionalstatistik_years,
        logger=orca.logger)
    extract_regionalstatistik.extract()


@meta(group='(8b) Pendler', title='Pendlerdaten importieren')
@orca.step()
def import_pendlerdaten(source_db: str,
                        subfolder_pendlerdaten: str,
                        pendlerdaten_years: List[str]):
    """
    Import neue Pendlerdaten aus Excel-Dateien in die Quelldatenbank
    Nur zu starten, wenn neue Pendlerdaten-Excel-Dateien auf den Server hochgeladen wurden!!
    """
    import_pendler = ImportPendlerdaten(db=source_db,
                                        subfolder=subfolder_pendlerdaten,
                                        pendlerdaten_years=pendlerdaten_years,
                                        logger=orca.logger)
    import_pendler.run()


@meta(group='(8b) Pendler', required=extract_regionalstatistik,
      title='Pendlerdaten extrahieren')
@orca.step()
def extract_pendlerdaten(source_db: str,
                         database: str,
                         pendlerdaten_gemeinden: str):
    """
    Filter Pendlerdaten mit Quelle oder Ziel in Gemeinden
    """
    extract_pendler = ExtractPendler(
        source_db=source_db,
        destination_db=database,
        pendlerdaten_gemeinden=pendlerdaten_gemeinden,
        logger=orca.logger)
    extract_pendler.extract()


@meta(group='(8b) Pendler', required=extract_pendlerdaten,
      title='Pendlerspinne erzeugen')
@orca.step()
def create_pendlerspinne(database: str,
                         pendlerspinne_gebiete: str,
                         target_srid: int):
    """
    Erzeugt Pendler-Spinne im GIS
    """
    create_pendlerspinne = CreatePendlerSpinne(
        db=database,
        pendlerspinne_gebiete=pendlerspinne_gebiete,
        target_srid=target_srid,
        logger=orca.logger)
    create_pendlerspinne.run()


@meta(group='(8b) Pendler', required=extract_pendlerdaten,
      title='Pendlerdaten exportieren')
@orca.step()
def export_pendlerdaten(database: str):
    """
    Export Pendlerdaten in Excel-Dateien auf dem Server zum Download
    """
    export = ExportPendlerdaten(
        db=database,
        logger=orca.logger)
    export.export()


@meta(group='(9) TravelDemandModel', order=1,
      title='Verschneidungstool vorbereiten',
      description='Vorbereitung der Tabellen und Views für das '
      'Verschneidungstool')
@orca.step()
def prepare_verschneidungstool(source_db: str, database: str, target_srid: int):
    """
    prepare Verschneidungstool tables and views
    """
    prepare = PrepareVerschneidungstool(source_db=source_db,
                                        destination_db=database,
                                        logger=orca.logger)
    prepare.extract()

@meta(group='(9) TravelDemandModel', order=2, title='BASt extrahieren',
      description='BASt-Netzwerk und Verkehrszählungen aus der Quelldatenbank '
      'extrahieren')
@orca.step()
def extract_bast(source_db: str, database: str, target_srid: int):
    """
    Copy BASt-Network and Traffic counts
    """
    extract_bast = ExtractBASt(source_db=source_db,
                               destination_db=database,
                               logger=orca.logger)
    extract_bast.extract()
