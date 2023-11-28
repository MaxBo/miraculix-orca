import orca
from orcadjango.decorators import meta
from extractiontools.ausschnitt import Extract
from extractiontools.drop_db import DropDatabase
from osgeo import ogr

__parent_modules__ = [
    'extractiontools.injectables.database'
]


@meta(group='(1) Project', order=1, title='Datenbank erstellen',
      description='Erstellen der Zieldatenbank.')
@orca.step()
def create_db(target_srid: str, project_area: ogr.Geometry, database: str):
    """
    (re)create the target database
    and copy the selected files
    """

    extract = Extract(destination_db=database,
                      target_srid=target_srid,
                      logger=orca.logger)
    extract.recreate_db()
    extract.set_target_boundary(project_area)
    extract.update_boundaries()


@meta(group='(1) Project', order=2, title='Datenbank löschen',
      description='Zieldatenbank und ihre Inhalte löschen.')
@orca.step()
def drop_db(database: str):
    """
    drop the database and its contents
    """
    extract = DropDatabase(destination_db=database, logger=orca.logger)
    extract.extract()


@meta(group='(1) Project', order=3, requires=create_db, title='Zugriff gewähren',
      description='''Gewährt <b>Lese- und Schreibrechte</b> in der Zieldatenbank für
      ausgewählte Nutzer:innen. Dies wirkt sich nur auf <b>bestehende Schemata</b> aus.
      Der Schritt sollte daher zum Schluss ausgeführt werden. <br>
      Der Zugriff auf ein Schema wird bei erneuter Erzeugung widerrufen (z.B. werden
      die Zugriffsrechte zum Schema "osm" zurückgesetzt, wenn der Schritt
      "extract_osm" erneut ausgeführt wird) ''')
@orca.step()
def grant_access(database: str, db_users: list):
    '''
    Grant read and write access to the database for the specified users.
    Only works for already existing schemas, so run this step last. Access for
    a schema is removed on recreation (e.g. access to schema 'osm' is revoked
    when re-running extract_osm)
    User "osm" always has access to all databases.
    '''
    extract = Extract(destination_db=database, logger=orca.logger)
    extract.grant_access(db_users)
