"""orca-steps to create networks"""

from typing import Dict
import os
import orca
from osgeo import ogr
from datetime import date
from orcadjango.decorators import meta
from extractiontools.connection import Login
from extractiontools.build_network_car import BuildNetwork
from extractiontools.build_graduated_network import BuildGraduatedNetwork
from extractiontools.build_network_walk_cycle import BuildNetworkWalkCycle
from extractiontools.scrape_stops import ScrapeStops
from extractiontools.bahn_routing import DBRouting
from extractiontools.scrape_timetable import ScrapeTimetable
from extractiontools.hafasdb2gtfs import HafasDB2GTFS
from extractiontools.network2pbf import CopyNetwork2Pbf, CopyNetwork2PbfTagged
from extractiontools.stop_otp_router import OTPServer
from extractiontools.copy2fgdb import Copy2FGDB
from typing import List

default_login = Login(
    host=os.environ.get('DB_HOST', 'localhost'),
    port=os.environ.get('DB_PORT', 5432),
    user=os.environ.get('DB_USER'),
    password=os.environ.get('DB_PASS', '')
)


@meta(group='(3) Networks', required=['extract_osm', 'extract_landuse'])
@orca.step()
def build_network_car(database: str,
                      chunksize: int,
                      limit4links: int,
                      links_to_find: float,
                      corine: str,
                      network_schema: str):
    """
    build a car network
    """
    build_network = BuildNetwork(db=database,
                                 network_schema=network_schema,
                                 limit=limit4links,
                                 chunksize=chunksize,
                                 links_to_find=links_to_find,
                                 corine=corine,
                                 logger=orca.logger)
    build_network.build()


@meta(group='(3) Networks', required=['extract_osm', 'extract_landuse'])
@orca.step()
def build_graduated_network_car(database: str,
                                chunksize: int,
                                limit4links: int,
                                links_to_find: float,
                                corine: str,
                                network_graduated_schema: str,
                                detailed_area: ogr.Geometry,
                                larger_area: ogr.Geometry):
    """
    build a graduated car network, detailed in the detailed area and only main roads in the larger area
    """
    build_network = BuildGraduatedNetwork(db=database,
                                          network_schema=network_graduated_schema,
                                          limit=limit4links,
                                          chunksize=chunksize,
                                          links_to_find=links_to_find,
                                          corine=corine,
                                          logger=orca.logger,
                                          detailed_network_area=detailed_area,
                                          larger_network_area=larger_area,
                                          )
    build_network.build()


@meta(group='(3) Networks', required=['extract_osm', 'extract_landuse'])
@orca.step()
def build_network_fr(database: str,
                     chunksize: int,
                     limit4links: int,
                     links_to_find: float,
                     corine: str,
                     routing_walk: bool,
                     network_fr_schema: str,
                     detailed_area: str,
                     ):
    """
    build a walk and cycle network
    """
    build_network = BuildNetworkWalkCycle(
        db=database,
        network_schema=network_fr_schema,
        limit=limit4links,
        chunksize=chunksize,
        links_to_find=links_to_find,
        corine=corine,
        routing_walk=routing_walk,
        logger=orca.logger,
        detailed_network_area=detailed_area,
    )
    build_network.build()


@meta(group='(4) Public Transport', order=1, required='create_db')
@orca.step()
def extract_stops(database: str):
    """
    extract public stops from database
    """
    scrape = ScrapeStops(database, logger=orca.logger)
    scrape.extract()


@meta(group='(4) Public Transport', order=2, required='create_db')
@orca.step()
def scrape_stops(database: str):
    """
    scrape public stops from Deutsche Bahn web interface
    """
    scrape = ScrapeStops(destination_db=database, logger=orca.logger)
    scrape.scrape()


@meta(group='(4) Public Transport', order=5, required='scrape_stops or extract_stops')
@orca.step()
def scrape_db_fastest_routes(database: str, destinations_db_routing: str,
                             date_db_routing: date, times_db_routing: List[int],
                             distance_db_routing):
    """
    scrape fastest routes from Deutsche Bahn at given day in between all scraped
    stops in table "haltestellen" as origins and the closest stops to the
    destinations in given table within defined radius around each origin.
    Results are stored in table db_{date}_{dest.-tablename} in the same schema
    with H_ID of origin stops, primary keys of destinations, fastest times
    in minutes and some meta info
    """
    routing = DBRouting(database, date_db_routing, times_db_routing,
                        logger=orca.logger)
    routing.scrape(destinations_db_routing, max_distance=distance_db_routing)


@meta(group='(4) Public Transport')
@orca.injectable()
def destinations_db_routing() -> str:
    """destination table ({schema}.{tablename}) for DB routing.
    Single(!) primary key is required to be defined in table.
    """
    return 'timetables.haltestellen'


@meta(group='(4) Public Transport')
@orca.injectable()
def distance_db_routing() -> int:
    """max. distance (beelines in meters) between origin
    and destination to route with Deutsche Bahn"""
    return 1000000000


@meta(group='(4) Public Transport')
@orca.injectable()
def date_db_routing() -> date:
    """date for the Deutsche Bahn routing"""
    return date.today()


@meta(group='(4) Public Transport')
@orca.injectable()
def times_db_routing() -> List[int]:
    """list of times (full hours) for the Deutsche Bahn routing on the day of
    "date_db_routing" """
    return [9, 13, 17]


@meta(group='(4) Public Transport')
@orca.injectable()
def date_timetable() -> date:
    """date for the timetable"""
    return date.today()


@meta(group='(4) Public Transport')
@orca.injectable()
def recreate_timetable_tables() -> bool:
    """recreate tables for timetables"""
    return False


@meta(group='(4) Public Transport', order=3,
      required='scrape_stops or extract_stops')
@orca.step()
def scrape_timetables(database: str, source_db: str,
                      date_timetable: str,
                      recreate_timetable_tables: bool):
    """
    Scrape timetables from Deutsche Bahn
    """
    scrape = ScrapeTimetable(destination_db=database, logger=orca.logger,
                             date=date_timetable, source_db=source_db,
                             recreate_tables=recreate_timetable_tables)
    scrape.scrape()


@meta(group='(4) Public Transport')
@orca.injectable()
def gtfs_only_one_day() -> bool:
    """gtfs valid only on the given day?"""
    return False


@meta(group='(4) Public Transport')
@orca.injectable()
def tbl_kreise() -> str:
    """table with the county geometries"""
    return 'verwaltungsgrenzen.krs_2018_12'


@meta(group='(4) Public Transport', order=4, required=scrape_timetables)
@orca.step()
def timetables_gtfs(database: str,
                    date_timetable: str,
                    gtfs_only_one_day: bool,
                    base_path: str,
                    subfolder_otp: str,
                    tbl_kreise: str):
    """
    Export Timetables as GTFS-file
    """
    hafas = HafasDB2GTFS(db=database,
                         date=date_timetable,
                         only_one_day=gtfs_only_one_day,
                         base_path=base_path,
                         subfolder=subfolder_otp,
                         tbl_kreise=tbl_kreise,
                         logger=orca.logger)
    hafas.convert()
    hafas.export_gtfs()


@meta(group='(6) OTP', editable_keys=True)
@orca.injectable()
def otp_networks() -> Dict[str, str]:
    """
    networks and subfolders for otp-networks
    returns a dictionary with the network-database schema as key
    and the subfolder for the pbf-files as values
    """
    return {'network': 'otp_car',
            'network_fr': 'otp_fr',
            }


@meta(group='(5) Export', required=[build_network_car, build_network_fr])
@orca.step()
def copy_network_pbf(database: str,
                     otp_networks: Dict[str, str]):
    """copy the osm networkdata to a pbf file"""
    for network_schema, subfolder_pbf in otp_networks.items():
        copy2pbf = CopyNetwork2Pbf(database,
                                   network_schema=network_schema,
                                   subfolder_pbf=subfolder_pbf,
                                   logger=orca.logger)
        copy2pbf.copy()


@meta(group='(5) Export', required=[build_network_car, build_network_fr])
@orca.step()
def copy_network_pbf_xml(database: str,
                         otp_networks: Dict[str, str]):
    """copy the osm networkdata to a pbf and .xml.bz file"""
    for network_schema, subfolder_pbf in otp_networks.items():
        copy2pbf = CopyNetwork2Pbf(database,
                                   network_schema=network_schema,
                                   subfolder_pbf=subfolder_pbf,
                                   as_xml=True,
                                   logger=orca.logger)
        copy2pbf.copy()


@meta(group='(5) Export', required=[build_network_car, build_network_fr])
@orca.step()
def copy_tagged_nw_pbf_xml(database: str,
                           otp_networks: Dict[str, str]):
    """
    copy the osm networkdata tagged with elevation
    to a pbf and .xml.bz file
    """
    for network_schema, subfolder_pbf in otp_networks.items():
        copy2pbf = CopyNetwork2PbfTagged(database,
                                         network_schema=network_schema,
                                         subfolder_pbf=subfolder_pbf,
                                         as_xml=True, logger=orca.logger)
        copy2pbf.copy()


@meta(group='(6) OTP')
@orca.injectable()
def otp_ports() -> Dict[str, int]:
    """A dict with the OTP Ports"""
    return {'port': 7789,
            'secure_port': 7788, }


@meta(group='(6) OTP')
@orca.injectable()
def otp_graph_subfolder() -> str:
    """subfolder with the otp graphs"""
    return 'otp_graphs'


@meta(group='(6) OTP')
@orca.injectable()
def otp_routers(database) -> Dict[str, str]:
    """subfolder with the otp graphs"""
    routers = {f'{database}_car': 'otp_car',
               f'{database}_fr': 'otp_fr',
               }
    return routers


@meta(group='(6) OTP')
@orca.injectable()
def start_otp_analyst() -> bool:
    """start otp router with analyst"""
    return True


@meta(group='(6) OTP', order=2)
@orca.step()
def start_otp_router(otp_ports: Dict[str, int],
                     base_path: str,
                     otp_graph_subfolder: str,
                     otp_routers: Dict[str, str],
                     start_otp_analyst: bool,
                     ):
    """Stop the running otp routers on the ports giben in `otp_ports`"""
    otp_server = OTPServer(ports=otp_ports,
                           base_path=base_path,
                           graph_subfolder=otp_graph_subfolder,
                           routers=otp_routers,
                           start_analyst=start_otp_analyst,
                           logger=orca.logger)
    otp_server.start()


@meta(group='(6) OTP', order=4)
@orca.step()
def stop_otp_router(otp_ports: Dict[str, int]):
    """Stop the running otp routers on the ports giben in `otp_ports`"""
    otp_server = OTPServer(ports=otp_ports,
                           logger=orca.logger)
    otp_server.stop()


@meta(group='(6) OTP', order=1)
@orca.step()
def create_router(otp_routers: Dict[str, str],
                  database: str,
                  base_path: str,
                  ):
    """Create otp graphs for the gives routers"""
    otp_server = OTPServer(ports=None,
                           base_path=base_path,
                           graph_subfolder='otp_graphs',
                           routers=otp_routers,
                           logger=orca.logger)
    for router_name, subfolder in otp_routers.items():
        build_folder = otp_server.get_otp_build_folder(database, subfolder)
        target_folder = os.path.join(otp_server.graph_folder, router_name)
        otp_server.create_router(build_folder, target_folder)


@meta(group='(3) Network', editable_keys=True)
@orca.injectable()
def network_layers() -> Dict[str, str]:
    """the network layers to export to the corresponding schema in a FGDB"""
    layers = {'autobahn': 'network_car',
              'hauptstr': 'network_car',
              'nebennetz': 'network_car',
              'faehren': 'network_car',
              'unaccessible_links': 'network_car',
              }
    return layers


@meta(group='(3) Network', editable_keys=True)
@orca.injectable()
def network_fr_layers() -> Dict[str, str]:
    """the network layers to export to the corresponding schema in a FGDB"""
    layers = {'links': 'network_fr',
              'unaccessible_links': 'network_fr',
              }
    return layers


@meta(group='(5) Export')
@orca.injectable()
def gdbname(database) -> str:
    """the name of the File Geodatabase"""
    return f'{database}.gdb'


@meta(group='(5) Export', required=build_network_car)
@orca.step()
def copy_network_car_fgdb(database: str,
                          network_layers: Dict[str, str]):
    """copy car network to a file-gdb"""
    copy2fgdb = Copy2FGDB(database,
                          layers=network_layers,
                          filename='network_car.gdb',
                          schema='network', logger=orca.logger)
    copy2fgdb.copy_layers('FileGDB')


@meta(group='(5) Export', required=build_network_fr)
@orca.step()
def copy_network_fr_fgdb(database: str,
                         network_fr_layers: Dict[str, str]):
    """copy walk and cycle network to a file-gdb"""
    copy2fgdb = Copy2FGDB(database, layers=network_fr_layers,
                          filename='network_fr.gdb',
                          schema='network_fr', logger=orca.logger)
    copy2fgdb.copy_layers('FileGDB')
