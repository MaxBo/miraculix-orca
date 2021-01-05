from typing import List, Dict
import ogr
import orca
import os
import re

from extractiontools.connection import Login, Connection
from orcadjango.decorators import meta

def get_foreign_tables(database, schema):
    login = Login(
        host=os.environ.get('FOREIGN_HOST', 'localhost'),
        port=os.environ.get('FOREIGN_PORT', 5432),
        user=os.environ.get('FOREIGN_USER'),
        password=os.environ.get('FOREIGN_PASS', ''),
        db=database
    )
    sql = f"""
    SELECT * FROM information_schema.tables
    WHERE table_schema = '{schema}'
    """
    with Connection(login=login) as conn:
        cursor = conn.cursor()
        cursor.execute(sql)
        rows = cursor.fetchall()
    return sorted([row.table_name for row in rows])

@meta(group='(1) Project')
@orca.injectable()
def database() -> str:
    """The name of the database"""
    return ''


@meta(group='(1) Project')
@orca.injectable()
def bbox_dict() -> Dict[str, float]:
    """The Bounding-Box of the Project"""
    return {'left': 9.0,
            'right': 9.1,
            'bottom': 54.5,
            'top': 54.6}


@meta(group='Areas', order=1)
@orca.injectable()
def project_area() -> ogr.Geometry:
    """The default area of the project"""
    ring = ogr.Geometry(ogr.wkbLinearRing)
    ring.AddPoint(9.0, 54.6)
    ring.AddPoint(9.1, 54.6)
    ring.AddPoint(9.1, 54.5)
    ring.AddPoint(9.0, 54.5)
    ring.AddPoint(9.0, 54.6)
    geom = ogr.Geometry(ogr.wkbPolygon)
    geom.AddGeometry(ring)
    return geom


@meta(group='(1) Project')
@orca.injectable()
def target_srid() -> int:
    """The EPSG-Code of the geodata to be created"""
    return 25832


@meta(group='Database')
@orca.injectable()
def source_db() -> str:
    """The name of the base-database"""
    return 'europe'


@meta(hidden=True)
@orca.injectable()
def verwaltungsgrenzen_tables_choices(source_db) -> List[str]:
    return get_foreign_tables(source_db, 'verwaltungsgrenzen')


@meta(group='Tables', choices=verwaltungsgrenzen_tables_choices)
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


@meta(hidden=True)
@orca.injectable()
def gmes_choices(source_db) -> List[str]:
    tables = get_foreign_tables(source_db, 'landuse')
    regex = 'ua[0-9]{4}$'
    return [t for t in tables if re.match(regex, t)]


@meta(group='Tables', choices=gmes_choices)
@orca.injectable()
def gmes() -> List[str]:
    """The Urban Atlas tables from the schema `landuse` to copy"""
    return ['ua2012']


@meta(hidden=True)
@orca.injectable()
def corine_choices(source_db) -> List[str]:
    tables = get_foreign_tables(source_db, 'landuse')
    regex = 'clc[0-9]{2}$'
    return [t for t in tables if re.match(regex, t)]


@meta(group='Tables', choices=corine_choices)
@orca.injectable()
def corine() -> List[str]:
    """The Corine Landcover tables from the schema `landuse` to copy"""
    return ['clc18']


@meta(group='Export')
@orca.injectable()
def base_path() -> str:
    """The basepath on the Miraculix-Server where the data is exported to"""
    return r'~/gis/projekte'


@meta(group='Export')
@orca.injectable()
def subfolder_tiffs() -> str:
    """The subfolder on the Miraculix-Server under the base_path
    where tiffs are exported to"""
    return 'tiffs'


@meta(group='Export')
@orca.injectable()
def subfolder_otp() -> str:
    """subfolder for the OpenTripPlanner"""
    return 'otp'


@meta(group='Network')
@orca.injectable()
def limit4links() -> int:
    """
    limit the creation of links to limit4links * chunksize
    if limit4links is 0, create all links
    """
    return 0


@meta(group='Network')
@orca.injectable()
def chunksize() -> int:
    """number of links to calculate in a chunk"""
    return 1000


@meta(group='Network')
@orca.injectable()
def links_to_find() -> float:
    """proportion of all network links to be found from starting point"""
    return 0.25


@meta(group='Network')
@orca.injectable()
def routing_walk() -> bool:
    """routing for walking"""
    return False
