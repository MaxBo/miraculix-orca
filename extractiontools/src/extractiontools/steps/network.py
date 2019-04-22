from typing import List
import orca
from extractiontools.injectables.database import Login
from extractiontools.build_network_car import BuildNetwork
from extractiontools.build_network_walk_cycle import BuildNetworkWalkCycle
from extractiontools.scrape_stops import ScrapeStops
from extractiontools.scrape_timetable import ScrapeTimetable

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
