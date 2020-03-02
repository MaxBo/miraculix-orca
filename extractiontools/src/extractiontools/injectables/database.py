from typing import List, Dict

import orca
from orcadjango.decorators import group
from extractiontools.connection import Login


@group('Database')
@orca.injectable()
def username() -> str:
    return 'osm'


@group('Database')
@orca.injectable()
def host() -> str:
    return 'localhost'


@group('Database')
@orca.injectable()
def port() -> int:
    return 5432


@group('Database')
@orca.injectable()
def password() -> str:
    return ''


@group('Project')
@orca.injectable()
def project() -> str:
    return 'myproject'


@group('Database')
@orca.injectable()
def login(host, port, username, password, project) -> Login:
    return Login(host=host,
                 port=port,
                 user=username,
                 password=password,
                 db=project)


@group('Project')
@orca.injectable()
def bbox_dict() -> Dict[str, float]:
    return {'left': 9.0,
            'right': 9.1,
            'bottom': 54.5,
            'top': 54.6}


@group('Database')
@orca.injectable()
def source_db() -> str:
    return 'europe'


@group('Project')
@orca.injectable()
def source_srid() -> int:
    return 4326


@group('Project')
@orca.injectable()
def target_srid() -> int:
    return 25832


@group('Tables')
@orca.injectable()
def verwaltungsgrenzen_tables() -> List[str]:
    tables = ['gem_2018_12',
              'vwg_2018_12',
              'krs_2018_12',
              'lan_2018_12',
              'gem_2014_ew_svb',
              'plz_2016']
    return tables


@group('Tables')
@orca.injectable()
def gmes() -> List[str]:
    return ['ua2012']


@group('Tables')
@orca.injectable()
def corine() -> List[str]:
    return ['clc18']


@group('Export')
@orca.injectable()
def base_path() -> str:
    return r'~/gis/projekte'


@group('Export')
@orca.injectable()
def subfolder_tiffs() -> str:
    return 'tiffs'


@group('Export')
@orca.injectable()
def subfolder_otp() -> str:
    """subfolder for the OpenTripPlanner"""
    return 'otp'


@group('Network')
@orca.injectable()
def limit4links() -> int:
    """
    limit the creation of links to limit4links * chunksize
    if limit4links is 0, create all links
    """
    return 0


@group('Network')
@orca.injectable()
def chunksize() -> int:
    """number of links to calculate in a chunk"""
    return 1000


@group('Network')
@orca.injectable()
def links_to_find() -> float:
    """proportion of all network links to be found from starting point"""
    return 0.25


@group('Network')
@orca.injectable()
def routing_walk() -> bool:
    """routing for walking"""
    return False
