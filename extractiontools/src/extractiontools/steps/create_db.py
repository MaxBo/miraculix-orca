import orca
from orcadjango.decorators import meta
from extractiontools.ausschnitt import Extract
from extractiontools.drop_db import DropDatabase
from osgeo import ogr

__parent_modules__ = [
    'extractiontools.injectables.database'
]


@meta(group='(1) Project', order=1, title='Datenbank erstellen')
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


@meta(group='(1) Project', order=2)
@orca.step()
def drop_db(database: str):
    """
    drop the database and its contents
    """
    extract = DropDatabase(destination_db=database, logger=orca.logger)
    extract.extract()


@meta(group='(1) Project', order=3, requires=create_db)
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
