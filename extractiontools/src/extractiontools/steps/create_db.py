import orca
from orcadjango.decorators import group
from extractiontools.master import BBox
from extractiontools.ausschnitt import ExtractMeta
from extractiontools.drop_db import DropDatabase

__parent_modules__ = [
    'extractiontools.injectables.database'
]


@group('CreateProject', order=1)
@orca.step()
def create_db(target_srid: str, bbox_dict: dict, database: str):
    """
    (re)-create the target database
    and copy the selected files
    """
    bbox = BBox(**bbox_dict)

    extract = Extract(destination_db=project,
                      target_srid=target_srid)
    extract.get_target_boundary(bbox)
    extract.recreate_db()
    #extract.extract()


@group('DeleteProject', order=1)
@orca.step()
def drop_db(database: str):
    """
    drop the database if this is allowed and remove metadata
    """

    extract = DropDatabase(destination_db=project)
    extract.extract()