import orca
from orcadjango.decorators import meta
from extractiontools.ausschnitt import Extract
from extractiontools.drop_db import DropDatabase
from osgeo import ogr

__parent_modules__ = [
    'extractiontools.injectables.database'
]


@meta(group='(1) Projekt', order=1, title='Datenbank erstellen',
      description='Erstellen der Zieldatenbank.')
@orca.step()
def create_db(target_srid: str, project_area: ogr.Geometry, database: str,
              db_status):
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


@meta(group='(1) Projekt', order=2, title='Datenbank löschen',
      description='Löscht Zieldatenbank und ihre Inhalte komplett.',
      required=create_db)
@orca.step()
def drop_db(database: str, db_status):
    """
    drop the database and its contents
    """
    extract = DropDatabase(destination_db=database, logger=orca.logger)
    extract.extract()


@meta(title='Datenbank löschen', description='Die Datenbank nach der erfolgreichen Archivierung löschen.', scope='step')
@orca.injectable()
def remove_db_after_archiving() -> bool:
    """routing for walking"""
    return False

@meta(group='(1) Projekt', order=4, required=create_db, title='Datenbank archivieren',
      description='Archiviert die Zieldatenbank als Dump')
@orca.step()
def archive_db(database: str, db_status, remove_db_after_archiving):
    '''
    '''
    extract = Extract(destination_db=database, logger=orca.logger)
    # extract.grant_access()


@meta(group='(1) Projekt', order=5, required=create_db, title='Zugriff gewähren',
      description='Stellt die Zieldatenbank aus dem archivierten Dump wieder her. Die Datenbank muss vorher '
                  'gelöscht worden sein.')
@orca.step()
def restore_db(database: str, db_status):
    '''
    '''
    extract = Extract(destination_db=database, logger=orca.logger)
    # extract.grant_access()

@meta(group='(1) Projekt', order=3, required=create_db, title='Zugriff gewähren',
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

