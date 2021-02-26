import orca
from orcadjango.decorators import meta
from extractiontools.ausschnitt import Extract
from extractiontools.drop_db import DropDatabase
from extractiontools.connection import DBApp
import ogr

__parent_modules__ = [
    'extractiontools.injectables.database'
]


@meta(group='(1) Project', order=1)
@orca.step()
def create_db(target_srid: str, project_area: ogr.Geometry, database: str,
              db_users: list):
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
    if db_users:
        extract.grant_access(db_users)


@meta(group='(1) Project', order=2)
@orca.step()
def drop_db(database: str):
    """
    drop the database and its contents
    """
    extract = DropDatabase(destination_db=database, logger=orca.logger)
    extract.extract()


@meta(group='(1) Project', order=3)
@orca.step()
def grant_access(database: str, db_users: list):
    '''
    Grant read and write access to the database for the specified users.
    '''
    extract = Extract(destination_db=database, logger=orca.logger)
    extract.grant_access(db_users)