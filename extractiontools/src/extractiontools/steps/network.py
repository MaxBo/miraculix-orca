import orca
from extractiontools.injectables.database import Login
from extractiontools.build_network_car import BuildNetwork
from extractiontools.build_network_walk_cycle import BuildNetworkWalkCycle
from extractiontools.scrape_stops import ScrapeStops
from extractiontools.scrape_timetable import ScrapeTimetable
from extractiontools.hafasdb2gtfs import HafasDB2GTFS
from extractiontools.network2pbf import CopyNetwork2Pbf


@orca.step()
def build_network_car(login: Login,
                      chunksize: int,
                      limit4links: int,
                      links_to_find: float,
                      corine: str):
    """
    build a car network
    """
    build_network = BuildNetwork(db=login.db,
                                 network_schema='network',
                                 limit=limit4links,
                                 chunksize=chunksize,
                                 links_to_find=links_to_find,
                                 corine=corine)
    build_network.login = login
    build_network.build()


@orca.step()
def build_network_fr(login: Login,
                     chunksize: int,
                     limit4links: int,
                     links_to_find: float,
                     corine: str,
                     routing_walk: bool):
    """
    build a walk and cycle network
    """
    build_network = BuildNetworkWalkCycle(db=login.db,
                                          network_schema='network_fr',
                                          limit=limit4links,
                                          chunksize=chunksize,
                                          links_to_find=links_to_find,
                                          corine=corine,
                                          routing_walk=routing_walk)
    build_network.login = login
    build_network.build()


@orca.step()
def extract_stops(login: Login, source_db: str):
    """
    extract Stops from master-DB
    """
    scrape = ScrapeStops(source_db=source_db,
                         db=login.db)
    scrape.set_login01(login, source_db)
    scrape.get_target_boundary_from_dest_db()
    scrape.extract()


@orca.step()
def scrape_stops(login: Login,  source_db: str):
    """
    Scrape Stops from DB
    """
    scrape = ScrapeStops(db=login.db)
    scrape.set_login01(login, source_db)
    scrape.get_target_boundary_from_dest_db()
    scrape.scrape()


@orca.injectable()
def date_timetable() -> str:
    """date for the timetable"""
    return '02.05.2019'


@orca.injectable()
def recreate_tables() -> bool:
    """recreate tables for timetables"""
    return False


@orca.step()
def scrape_timetables(login: Login,
                      date_timetable: str,
                      recreate_tables: bool,
                      source_db: str):
    """
    Scrape Stops from DB
    """
    scrape = ScrapeTimetable(db=login.db,
                             date=date_timetable,
                             source_db=source_db,
                             recreate_tables=recreate_tables)
    scrape.set_login01(login, source_db)
    scrape.get_target_boundary_from_dest_db()
    scrape.scrape()


@orca.injectable()
def gtfs_only_one_day() -> bool:
    """gtfs valid only on the given day?"""
    return False


@orca.injectable()
def tbl_kreise() -> str:
    """table with the county geometries"""
    return 'verwaltungsgrenzen.krs_2018_12'



@orca.step()
def timetables_to_gtfs(login: Login,
                       date_timetable: str,
                       gtfs_only_one_day: bool,
                       base_path: str,
                       subfolder_otp: str,
                       tbl_kreise: str):
    """
    Export Timetables as GTFS-file
    """
    hafas = HafasDB2GTFS(db=login.db,
                         date=date_timetable,
                         only_one_day=gtfs_only_one_day,
                         base_path=base_path,
                         subfolder=subfolder_otp,
                         tbl_kreise=tbl_kreise)
    hafas.login1 = login
    hafas.get_target_boundary_from_dest_db()
    hafas.convert()
    hafas.export_gtfs()


@orca.injectable()
def subfolder_pbf() -> str:
    """subfolder to store pbf files"""
    return 'pbf'


@orca.injectable()
def network_schema() -> str:
    """database schema for the routable network"""
    return 'network'



@orca.step()
def copy_network_to_pbf(login: Login,
                        network_schema: str,
                        subfolder_pbf: str):
    """copy the osm networkdata to a pbf file"""
    copy2pbf = CopyNetwork2Pbf(login=login,
                               network_schema=network_schema,
                               subfolder_pbf=subfolder_pbf)
    copy2pbf.copy()


@orca.step()
def copy_network_to_pbf_and_xml(login: Login,
                                network_schema: str,
                                subfolder_pbf: str):
    """copy the osm networkdata to a pbf and .xml.bz file"""
    copy2pbf = CopyNetwork2Pbf(login=login,
                               network_schema=network_schema,
                               subfolder_pbf=subfolder_pbf,
                               as_xml=True)
    copy2pbf.copy()
