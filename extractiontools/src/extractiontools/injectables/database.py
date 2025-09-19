from typing import List
from osgeo import ogr
import orca
import os
import re

from extractiontools.archive import Archive
from extractiontools.connection import Login, Connection
from extractiontools.utils.google_api import GooglePlacesAPI

try:
    from orcadjango.decorators import meta
except:
    def meta(**kwargs):
        '''
        mockup decorator if extractiontools are used outside of orcadjango
        '''
        def decorator(func):
            pass
        return decorator


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
    return _get_tables(login, schema)

def db_exists(db_name: str) -> bool:
    login = create_login()
    with Connection(login=login) as conn:
        cursor = conn.cursor()
        sql = 'SELECT datname FROM pg_catalog.pg_database WHERE datname = %s;'
        cursor = conn.cursor()
        cursor.execute(sql, (db_name, ))
        rows = cursor.fetchall()
        exists = len(rows) > 0
    return exists

def get_tables(database, schema) -> dict:
    if not database:
        return {}
    login = create_login(database)
    if not db_exists(database):
        return {}
    return _get_tables(login, schema)

def _get_tables(login, schema) -> dict:
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


@meta(group='(1) Projekt', unique=True, order=1,
      title='Datenbank', description='Der Name der Zieldatenbank',
      regex="^[A-Za-z_@#]{1}[A-Za-z0-9_\-]{0,127}$",
      regex_help='Das erste Zeichen darf ein Buchstabe, "@" ,"_" oder "#" sein. '
      'Der Rest sind Buchstaben, Zahlen, "@", "_" oder "-". '
      'Freizeichen oder Umlaute sind nicht erlaubt. '
      'Die maximale Länge beträgt 128 Zeichen.')
@orca.injectable()
def database() -> str:
    """The name of the database"""
    return ''


@meta(group='(1) Projekt', refresh='always', order=2,
      title='Datenbankstatus', description='Status der Datenbank')
@orca.injectable()
def db_status(database) -> dict:
    if not database:
        return  {'existiert': False}

    status = {}
    exists = db_exists(database)
    status['existiert'] = exists
    if exists:
        login = create_login(database)
        with Connection(login=login) as conn:
            cursor = conn.cursor()
            sql = 'SELECT pg_size_pretty(pg_database_size(%s));'
            cursor.execute(sql, (database, ))
            r = cursor.fetchone()
            status['Datenbankgröße'] = r.pg_size_pretty
            sql = 'select schema_name from information_schema.schemata;'
            cursor.execute(sql)
            rows = cursor.fetchall()
            status['Schemas'] = ', '.join([r.schema_name for r in rows])
    archive = Archive(database)
    exists = archive.exists()
    status['Archiv'] = archive.fn if exists else 'nicht vorhanden'
    if exists:
        status['Datum Archivierung'] = archive.date_str()
    return status


@meta(hidden=True, refresh='always')
@orca.injectable()
def user_choices() -> List[str]:
    login = create_login()
    sql = 'SELECT rolname FROM pg_catalog.pg_roles WHERE rolsuper = False;'
    with Connection(login=login) as conn:
        cursor = conn.cursor()
        cursor.execute(sql)
        rows = cursor.fetchall()
    return [r.rolname for r in rows if not r.rolname.startswith('pg_')]


@meta(group='(1) Projekt', order=6, choices=user_choices,
      title='Datenbanknutzer:innen', description='Diesen Nutzer:innen wird Zugriff auf '
      'die Datenbank und ihre Tabellen gewährt. Die Auswahl beschränkt sich '
      'auf bereits angelegte Rollen.')
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

@meta(group='(1) Projekt', order=4,
      title='Projektgebiet',
      description='Das Projektgebiet. Die Daten werden auf dieses '
      'Gebiet zugeschnitten.')
@orca.injectable()
def project_area() -> ogr.Geometry:
    """The default area of the project"""
    return None


@meta(group='(1) Projekt', order=3, title='Projektion',
      description='EPSG-Code des Koordinatenreferenzsystems, in das alle '
      'Geodaten transformiert werden, die in der Zieldatenbank abgelegt werden.')
@orca.injectable()
def target_srid() -> int:
    """The EPSG-Code of the geodata to be created"""
    return 25832


@meta(group='(1) Projekt', order=5, choices=['europe'], title='Quelldatenbank',
      description='Der Name der Datenbank, aus der die Daten extrahiert werden.')
@orca.injectable()
def source_db() -> str:
    """The name of the base-database to extract data from"""
    return 'europe'

@meta(hidden=True, refresh='always')
@orca.injectable()
def extracted_vwg_tables_choices(database) -> dict:
    return get_tables(database, 'verwaltungsgrenzen')

@meta(hidden=True, refresh='always')
@orca.injectable()
def verwaltungsgrenzen_tables_choices(source_db) -> dict:
    return get_foreign_tables(source_db, 'verwaltungsgrenzen')


@meta(group='(2) Tabellen', choices=verwaltungsgrenzen_tables_choices,
      title='Verwaltungsgrenzen',
      description='Auswahl der Tabellen mit Verwaltungsgrenzen, die '
      'extrahiert werden sollen')
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


@meta(group='(2) Tabellen', choices=firms_tables_choices, title='Firmen',
      description='Auswahl der Tabellen, die aus dem Schema "firms" '
      'extrahiert werden sollen')
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


@meta(group='(2) Tabellen', choices=gmes_choices,
      title='GMES-Daten',
      description='Auswahl der Tabellen des "Urban Atlas", die extrahiert '
      'werden sollen')
@orca.injectable()
def gmes() -> List[str]:
    """The Urban Atlas tables from the schema `landuse` to copy"""
    return ['ua2018']


@meta(hidden=True, refresh='always')
@orca.injectable()
def corine_choices(source_db) -> dict:
    tables = get_foreign_tables(source_db, 'landuse')
    regex = 'clc[0-9]{2}$'
    return {k: v for k, v in tables.items() if re.match(regex, k)}


@meta(group='(2) Tabellen', choices=corine_choices,  title='CORINE-Daten',
      description='Auswahl der Tabellen des CORINE Land Cover, die extrahiert '
      'werden sollen')
@orca.injectable()
def corine() -> List[str]:
    """The Corine Landcover tables from the schema `landuse` to copy"""
    return ['clc18']


@meta(group='(5) Export', hidden=True)
@orca.injectable()
def base_path() -> str:
    """The basepath on the Miraculix-Server where the data is exported to"""
    return r'/root/gis/projekte'


@meta(group='(5) Export', title='TIFF-Ordner', description='Unterordner des '
      'Basispfads auf dem Server, in dem die exportierten TIFFs abgelegt werden')
@orca.injectable()
def subfolder_tiffs() -> str:
    """The subfolder on the Miraculix-Server under the base_path
    where tiffs are exported to"""
    return 'tiffs'


@meta(group='(5) Export', title='OTP-Ordner', description='Unterordner des '
      'Basispfads auf dem Server, in dem die Dateien des OpenTripPlanners '
      'abgelegt werden')
@orca.injectable()
def subfolder_otp() -> str:
    """subfolder for the OpenTripPlanner"""
    return 'otp'


@meta(group='(8b) Pendlerdaten', title='Pendlerdaten-Ordner',
      description='Unterordner des Basispfads auf dem Server, in dem die '
      'Exceldateien mit den Pendlerdaten abgelegt werden')
@orca.injectable()
def subfolder_pendlerdaten() -> str:
    """subfolder for the Pendlerdaten-Excelfiles"""
    return 'Pendlerdaten'


@meta(group='(8b) Pendlerdaten', title='Jahre mit Pendlerdaten',
      description='Jahre, die als Pendlerdaten importiert werden')
@orca.injectable()
def pendlerdaten_years() -> List[str]:
    """Years to import as Pendlerdaten"""
    return ['2019', '2020']


@meta(group='(8a) Regionalstatistik', title='Jahre der Regionalstatistik',
      description='Jahre, die aus der Regionalstatistik importiert werden')
@orca.injectable()
def regionalstatistik_years() -> List[str]:
    """Years to import as Regionalstatistik"""
    return ['2020', '2021']


@meta(group='(8a) Regionalstatistik', title='Gemeinden der Regionalstatistik',
      choices=extracted_vwg_tables_choices,
      description='Layer mit Gemeinden, für die die Statistiken importiert werden')
@orca.injectable()
def regionalstatistik_gemeinden() -> str:
    """Gemeindelayer for Regionalstatistik"""
    return ''


@meta(group='(8b) Pendlerdaten', title='Gemeinden mit Pendlerdaten',
      choices=extracted_vwg_tables_choices,
      description='Layer mit Gemeinden, für die die Pendlerdaten importiert werden')
@orca.injectable()
def pendlerdaten_gemeinden() -> str:
    """Gemeindelayer for Pendlerdaten"""
    return ''


@meta(group='(8b) Pendlerdaten', title='Gebiete der Pendlerspinne',
      choices=extracted_vwg_tables_choices)
@orca.injectable()
def pendlerspinne_gebiete() -> str:
    """Layer mit den Gebieten für die Pendlerspinne"""
    return ''


@meta(group='(3) Netzwerk', title='Netzwerkschema',
      description='Das Datenbankschema, in dem die Netzwerkdaten abgelegt werden')
@orca.injectable()
def network_schema() -> str:
    """
    The network-schema in the database
    """
    return 'network'


@meta(group='(3) Netzwerk', title='Netzwerkschema Fuß/Fahrrad',
      description='Das Datenbankschema, in dem die Netzwerkdaten der Modi '
      '"zu Fuß" und Fahrrad abgelegt werden')
@orca.injectable()
def network_fr_schema() -> str:
    """
    The network-schema for walk/cycling in the database
    """
    return 'network_fr'


@meta(group='(3) Netzwerk', title='Netzwerkschema abgestuft',
      description='Das Datenbankschema, in dem die Daten des abgestuften Netzwerks '
      'abgelegt werden')
@orca.injectable()
def network_graduated_schema() -> str:
    """
    The network-schema for a graduated network in the database
    """
    return 'network_grad'


@meta(group='(3) Netzwerk')
@orca.injectable()
def limit4links() -> int:
    """
    limit the creation of links to limit4links * chunksize
    if limit4links is 0, create all links
    """
    return 0


@meta(group='(3) Netzwerk')
@orca.injectable()
def chunksize() -> int:
    """number of links to calculate in a chunk"""
    return 1000


@meta(group='(3) Netzwerk')
@orca.injectable()
def links_to_find() -> float:
    """proportion of all network links to be found from starting point"""
    return 0.25


@meta(group='(3) Netzwerk', title='Routing zu Fuß',
      description='Modus "zu Fuß" routen (ja) oder nicht (nein)')
@orca.injectable()
def routing_walk() -> bool:
    """routing for walking"""
    return False


@meta(group='(7) Google Places', title='Schlüssel Places-API',
      description='Schlüssel für die Google-Places-API')
@orca.injectable()
def google_key() -> str:
    '''Google API key'''
    return ''


@meta(group='(7) Google Places', title='Places-Ergebnistabelle',
      description='Name der Tabelle im Schema "google", in die die Ergebnisse '
      'der Google-Places-Suche geschrieben werden. EXISTIERENDE TABELLEN '
      'WERDEN ÜBERSCHRIEBEN!')
@orca.injectable()
def places_table() -> str:
    '''Name of table in schema "google" to store results of Google Places search
    in. EXISTING TABLE WILL BE OVERWRITTEN!'''
    return 'places'


@meta(group='(7) Google Places', title='Places-Keyword',
      description='Optionales Keyword für die Google-Places-Suche. Der Ausdruck '
      'wird mit allen Inhalten abgeglichen, die Google für die Orte indiziert hat. '
      'Das schließt unter anderem den Namen, den Typ, die Adresse, '
      'die Kundenrezensionen und weitere Third-Party-Inhalte mit ein.')
@orca.injectable()
def places_keyword() -> str:
    '''Optional keyword for Places search with Google. Term is matched against
    all content that Google has indexed for the places, including but not
    limited to name, type, and address, as well as customer reviews and other
    third-party content'''
    return ''


@meta(group='(7) Google Places', choices=['no restriction'] + GooglePlacesAPI.types,
      title='Places-Typ', description='Optional die Ergebnisse der '
      'Places-Suche auf Orte beschränken, die dem gewählten Typ entsprechen. '
      '"no restriction" wählen, um die Ergebnisse nicht zu beschränken.')
@orca.injectable()
def places_type() -> str:
    '''Optionally restrict the results of Places search with Google to places
    matching the specified type.'''
    return 'no restriction'


@meta(group='(7) Google Places', title='Places-Suchradius',
      description='Suchradius pro Google-Places-Abfrage in Metern '
      '(max. 50000 Meter). Das Projektgebiet wird in mehrere Abschnitte '
      'unterteilt, um es komplett mit Kreisen mit dem definierten Radius '
      'abdecken zu können. Je kleiner der Radius, desto mehr Abfragen müssen '
      'durchgeführt werden. <br>Grund für dieses Vorgehen ist, dass die Places-API '
      '<b>max. 60 Features pro Abfrage</b> zurückliefert. Wenn du viele '
      'Features in der Suche erwartest, setze den Radius entsprechend klein')
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


@meta(group='(3) Netzwerk', order=1, title='Gebiet mit feiner Netzwerkauflösung',
      description='''Gebiet, in dem das Wegenetz detailliert wird. Frei lassen,
      um den abgeleiteten Ausschnitt auf die Dimension des Projektgebiets zu setzen.''')
@orca.injectable()
def detailed_network_area() -> ogr.Geometry:
    """The area in which the network will be detailed.
    Leave empty to default it to the project area in the calculations."""
    return


@meta(group='(3) Netzwerk', order=2, refresh='always',
      title='Gebiet mit feiner Netzwerkauflösung (abgeleitet)',
      description='Tatsächliches Gebiet, in dem das Wegenetz detailliert abgebildet wird.')
@orca.injectable()
def detailed_area(detailed_network_area: ogr.Geometry,
                  project_area: ogr.Geometry) -> ogr.Geometry:
    """
    The area in which the network will be detailed,
    defaults to the project area, if no detailed_network_area is given
    """
    if detailed_network_area:
        return detailed_network_area
    return project_area


@meta(group='(3) Netzwerk', order=3,
      title='Gebiet mit grober Netzwerkauflösung',
      description='''Gebiet, in dem das Wegenetz nur grob abgebildet wird (nur Hauptstraßen).
      Frei lassen, um den abgeleiteten Ausschnitt auf die Dimension des Projektgebiets zu setzen.''')
@orca.injectable()
def larger_network_area() -> ogr.Geometry:
    """The area where the network will only cover main roads.
    Leave empty to default it to the project area in the calculations."""
    return


@meta(group='(3) Netzwerk', order=4, refresh='always',
      title='Gebiet mit grober Netzwerkauflösung (abgeleitet)',
      description='Tatsächliches Gebiet, in dem das Wegenetz nur grob abgebildet wird (nur Hauptstraßen).')
@orca.injectable()
def larger_area(larger_network_area: ogr.Geometry,
                project_area: ogr.Geometry) -> ogr.Geometry:
    """
    The area in which the network will only cover main roads,
    defaults to the project area, if no larger_network_area is given
    """
    if larger_network_area:
        return larger_network_area
    return project_area
