import orca
from orcadjango.decorators import meta
from extractiontools.ausschnitt import Extract
from extractiontools.drop_db import DropDatabase
import ogr

__parent_modules__ = [
    'extractiontools.injectables.database'
]


@meta(group='(1) Project', order=1)
@orca.step()
def create_db(target_srid: str, project_area: ogr.Geometry, database: str):
    """
    (re)-create the target database
    and copy the selected files
    """

    extract = Extract(destination_db=database,
                      target_srid=target_srid,
                      logger=orca.logger)
    extract.recreate_db()
    extract.set_target_boundary(project_area)


@meta(group='(1) Project', order=1)
@orca.step()
def drop_db(database: str):
    """
    drop the database if this is allowed and remove metadata
    """
    extract = DropDatabase(destination_db=database, logger=orca.logger)
    extract.extract()