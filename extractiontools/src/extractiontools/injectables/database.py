from typing import List, Dict

import orca
from orcadjango.decorators import group
from extractiontools.connection import Login


@group('Database')
@orca.injectable()
def username() -> str:
    """The database username"""
    return 'osm'


@group('Database')
@orca.injectable()
def host() -> str:
    """The database host"""
    return 'localhost'


@group('Database')
@orca.injectable()
def port() -> int:
    """The database port"""
    return 5432


@group('Database')
@orca.injectable()
def password() -> str:
    """The database password"""
    return ''


@group('Project')
@orca.injectable()
def project() -> str:
    """The name of the project and the database"""
    return 'myproject'


#@group('Database')
#@orca.injectable()
#def login(host, port, username, password, project) -> Login:
    #"""The Login-connection"""
    #return Login(host=host,
                 #port=port,
                 #user=username,
                 #password=password,
                 #db=project)


@group('Project')
@orca.injectable()
def bbox_dict() -> Dict[str, float]:
    """The Bounding-Box of the Project"""
    return {'left': 9.0,
            'right': 9.1,
            'bottom': 54.5,
            'top': 54.6}


@group('Database')
@orca.injectable()
def source_db() -> str:
    """The name of the base-database"""
    return 'europe'


@group('Project')
@orca.injectable()
def source_srid() -> int:
    """The EPSG-Code of the geodata in the base database"""
    return 4326


@group('Project')
@orca.injectable()
def target_srid() -> int:
    """The EPSG-Code of the geodata zo be created"""
    return 25832


@group('Tables')
@orca.injectable()
def verwaltungsgrenzen_tables() -> List[str]:
    """A list of tables in the schema `verwaltungsgrenzen` to copy"""
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
    """The Urban Atlas tables from the schema `landuse` to copy"""
    return ['ua2012']


@group('Tables')
@orca.injectable()
def corine() -> List[str]:
    """The Corine Landcover tables from the schema `landuse` to copy"""
    return ['clc18']


@group('Export')
@orca.injectable()
def base_path() -> str:
    """The basepath on the Miraculix-Server where the data is exported to"""
    return r'~/gis/projekte'


@group('Export')
@orca.injectable()
def subfolder_tiffs() -> str:
    """The subfolder on the Miraculix-Server under the base_path
    where tiffs are exported to"""
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
