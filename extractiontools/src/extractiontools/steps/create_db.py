import orca
from extractiontools.master import BBox
from extractiontools.ausschnitt import ExtractMeta
from extractiontools.drop_db import DropDatabase
from extractiontools.injectables.database import Login

__parent_modules__ = ['extractiontools.injectables.database',
                      ]


@orca.step()
def create_db(source_db: str, target_srid: str, bbox_dict: dict, login: Login):
    """
    (re)-create the target database
    and copy the selected files
    """
    bbox = BBox(**bbox_dict)

    extract = ExtractMeta(destination_db=login.db,
                          target_srid=target_srid,
                          source_db =source_db)
    extract.set_login(**login.__dict__)
    extract.get_target_boundary(bbox)
    extract.recreate_db()
    extract.extract()


@orca.step()
def drop_db(source_db: str, login: Login):
    """
    drop the database if this is allowed and remove metadata
    """

    extract = DropDatabase(source_db=source_db,
                           destination_db=login.db,)
    extract.set_login(**login.__dict__)
    extract.extract()