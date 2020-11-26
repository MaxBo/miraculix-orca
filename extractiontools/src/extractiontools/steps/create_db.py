import orca
from orcadjango.decorators import meta
from extractiontools.master import BBox
from extractiontools.ausschnitt import Extract
from extractiontools.drop_db import DropDatabase

__parent_modules__ = [
    'extractiontools.injectables.database'
]


@meta(group='(1) Project', order=1)
@orca.step()
def create_db(target_srid: str, bbox_dict: dict, database: str):
    """
    (re)-create the target database
    and copy the selected files
    """
    bbox = BBox(**bbox_dict)

    extract = Extract(destination_db=database,
                      target_srid=target_srid,
                      logger=orca.logger)
    extract.recreate_db()
    extract.set_target_boundary(bbox)
    #extract.extract()


@meta(group='(1) Project', order=1)
@orca.step()
def drop_db(database: str):
    """
    drop the database if this is allowed and remove metadata
    """
    extract = DropDatabase(destination_db=database, logger=orca.logger)
    extract.extract()