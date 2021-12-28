from typing import List
import ogr
import orca
import os
import re

from extractiontools.connection import Login, Connection
from orcadjango.decorators import meta
from extractiontools.utils.google_api import GooglePlacesAPI


def create_foreign_login(database='postgres'):
    return Login(
        host=os.environ.get('FOREIGN_HOST', 'localhost'),
        port=os.environ.get('FOREIGN_PORT', 5432),
        user=os.environ.get('FOREIGN_USER'),
        password=os.environ.get('FOREIGN_PASS', ''),
        db=database
    )


def create_login(database='postgres'):
    return Login(
        host=os.environ.get('DB_HOST', 'localhost'),
        port=os.environ.get('DB_PORT', 5432),
        user=os.environ.get('DB_USER', 'postgres'),
        password=os.environ.get('DB_PASS', ''),
        db=database
    )


def get_foreign_tables(database, schema) -> dict:
    login = create_foreign_login(database)
    sql = f"""
    SELECT * FROM information_schema.tables
    WHERE table_schema = '{schema}'
    ORDER BY table_name;
    """
    with Connection(login=login) as conn:
        cursor = conn.cursor()
        cursor.execute(sql)
        rows = cursor.fetchall()
        tables = {row.table_name: '' for row in rows}

        sql = f'''
        SELECT d.description, c.relname FROM pg_catalog.pg_description as d
        join pg_catalog.pg_class as c on d.objoid = c.oid
        join pg_catalog.pg_namespace as n on c.relnamespace = n.oid
        where nspname='{schema}';
        '''
        cursor.execute(sql)
        rows = cursor.fetchall()
        for row in rows:
            tables[row.relname] = row.description
    return tables


@meta(group='(1) Project', unique=True, order=1,
      regex="^[A-Za-z_@#]{1}[A-Za-z0-9_\-]{0,127}$",
      regex_help="The first character can be a letter, @ , _ , or # . "
      "The rest is letters, numbers or @ , _ , - . "
      "No spaces or umlauts allowed. The maximum length is 128 characters.")
@orca.injectable()
def database() -> str:
    """The name of the database"""
    return ''


@meta(hidden=True, refresh='always')
@orca.injectable()
def user_choices(source_db) -> List[str]:
    login = create_login()
    sql = 'SELECT rolname FROM pg_catalog.pg_roles WHERE rolsuper = False;'
    with Connection(login=login) as conn:
        cursor = conn.cursor()
        cursor.execute(sql)
        rows = cursor.fetchall()
    return [r.rolname for r in rows if not r.rolname.startswith('pg_')]


@meta(group='(1) Project', order=4, choices=user_choices)
@orca.injectable()
def db_users() -> List[str]:
    return []


def dummy_polygon():
    ring = ogr.Geometry(ogr.wkbLinearRing)
    ring.AddPoint(9.0, 54.6)
    ring.AddPoint(9.1, 54.6)
    ring.AddPoint(9.1, 54.5)
    ring.AddPoint(9.0, 54.5)
    ring.AddPoint(9.0, 54.6)
    geom = ogr.Geometry(ogr.wkbPolygon)
    geom.AddGeometry(ring)
    return geom


@meta(group='(1) Project', order=3)
@orca.injectable()
def project_area() -> ogr.Geometry:
    """The default area of the project"""
    geom = dummy_polygon()
    return geom


@meta(group='(1) Project', order=2)
@orca.injectable()
def target_srid() -> int:
    """The EPSG-Code of the geodata to be created"""
    return 25832


@meta(group='(1) Project', order=4, choices=['europe'])
@orca.injectable()
def source_db() -> str:
    """The name of the base-database to extract data from"""
    return 'europe'


@meta(hidden=True, refresh='always')
@orca.injectable()
def verwaltungsgrenzen_tables_choices(source_db) -> dict:
    return get_foreign_tables(source_db, 'verwaltungsgrenzen')


@meta(group='(2) Extract-Tables', choices=verwaltungsgrenzen_tables_choices)
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


@meta(hidden=True, refresh='always')
@orca.injectable()
def firms_tables_choices(source_db) -> dict:
    return get_foreign_tables(source_db, 'firms')


@meta(group='(2) Extract-Tables', choices=firms_tables_choices)
@orca.injectable()
def firms_tables() -> List[str]:
    """A list of tables in the schema `firms` to copy"""
    tables = ['bedirect',
              'irb_stadtteile']
    return tables


@meta(hidden=True, refresh='always')
@orca.injectable()
def gmes_choices(source_db) -> dict:
    tables = get_foreign_tables(source_db, 'landuse')
    regex = 'ua[0-9]{4}$'
    return {k: v for k, v in tables.items() if re.match(regex, k)}


@meta(group='(2) Extract-Tables', choices=gmes_choices)
@orca.injectable()
def gmes() -> List[str]:
    """The Urban Atlas tables from the schema `landuse` to copy"""
    return ['ua2012']


@meta(hidden=True, refresh='always')
@orca.injectable()
def corine_choices(source_db) -> dict:
    tables = get_foreign_tables(source_db, 'landuse')
    regex = 'clc[0-9]{2}$'
    return {k: v for k, v in tables.items() if re.match(regex, k)}


@meta(group='(2) Extract-Tables', choices=corine_choices)
@orca.injectable()
def corine() -> List[str]:
    """The Corine Landcover tables from the schema `landuse` to copy"""
    return ['clc18']


@meta(group='(5) Export')
@orca.injectable()
def base_path() -> str:
    """The basepath on the Miraculix-Server where the data is exported to"""
    return r'~/gis/projekte'


@meta(group='(5) Export')
@orca.injectable()
def subfolder_tiffs() -> str:
    """The subfolder on the Miraculix-Server under the base_path
    where tiffs are exported to"""
    return 'tiffs'


@meta(group='(5) Export')
@orca.injectable()
def subfolder_otp() -> str:
    """subfolder for the OpenTripPlanner"""
    return 'otp'8


@meta(group='(8) Pendlerdaten')
@orca.injectable()
def subfolder_pendlerdaten() -> str:
    """subfolder for the Pendlerdaten-Excelfiles"""
    return 'pendlerdaten'


@meta(group='(3) Network')
@orca.injectable()
def network_schema() -> str:
    """
    The network-schema in the database
    """
    return 'network'


@meta(group='(3) Network')
@orca.injectable()
def network_fr_schema() -> str:
    """
    The network-schema for walk/cycling in the database
    """
    return 'network_fr'


@meta(group='(3) Network')
@orca.injectable()
def network_graduated_schema() -> str:
    """
    The network-schema for a graduated network in the database
    """
    return 'network_grad'


@meta(group='(3) Network')
@orca.injectable()
def limit4links() -> int:
    """
    limit the creation of links to limit4links * chunksize
    if limit4links is 0, create all links
    """
    return 0


@meta(group='(3) Network')
@orca.injectable()
def chunksize() -> int:
    """number of links to calculate in a chunk"""
    return 1000


@meta(group='(3) Network')
@orca.injectable()
def links_to_find() -> float:
    """proportion of all network links to be found from starting point"""
    return 0.25


@meta(group='(3) Network')
@orca.injectable()
def routing_walk() -> bool:
    """routing for walking"""
    return False


@meta(group='(7) Google')
@orca.injectable()
def google_key() -> str:
    '''Google API key'''
    return ''


@meta(group='(7) Google')
@orca.injectable()
def places_table() -> str:
    '''Name of table in schema "google" to store results of Google Places search
    in. EXISTING TABLE WILL BE OVERWRITTEN!'''
    return 'places'


@meta(group='(7) Google')
@orca.injectable()
def places_keyword() -> str:
    '''Optional keyword for Places search with Google. Term is matched against
    all content that Google has indexed for the places, including but not
    limited to name, type, and address, as well as customer reviews and other
    third-party content'''
    return ''


@meta(group='(7) Google', choices=['no restriction'] + GooglePlacesAPI.types)
@orca.injectable()
def places_type() -> str:
    '''Optionally restrict the results of Places search with Google to places
    matching the specified type.'''
    return 'no restriction'


@meta(group='(7) Google')
@orca.injectable()
def places_search_radius() -> int:
    '''Search radius per Google Places query in meters (max. 50000 meters).
    The project area will be rastered into several to cover the whole area with
    the defined radius. The smaller the radius the more search points are needed
    and the more requests will be done.
    The Places API will return max. 60 features per request. If you expect a lot
    of features per search set it as small as needed.
    '''
    return 1000


@meta(group='(3) Network', order=1)
@orca.injectable()
def detailed_network_area() -> ogr.Geometry:
    """The area in which the network will be detailed"""
    geom = dummy_polygon()
    return geom


@meta(group='(3) Network', order=2)
@orca.injectable()
def larger_network_area() -> ogr.Geometry:
    """The area where the network will only cover main roads"""
    geom = dummy_polygon()
    return geom
