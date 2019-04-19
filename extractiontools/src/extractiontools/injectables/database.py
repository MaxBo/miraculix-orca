from typing import List, Dict

import orca
from extractiontools.connection import Login


@orca.injectable()
def username() -> str:
    return 'osm'


@orca.injectable()
def host() -> str:
    return 'localhost'


@orca.injectable()
def port() -> int:
    return 5432


@orca.injectable()
def password() -> str:
    return ''


@orca.injectable()
def password() -> str:
    return ''


@orca.injectable()
def project() -> str:
    return 'myproject'


@orca.injectable()
def login(host, port, username, password, project) -> Login:
    return Login(host=host,
                 port=port,
                 user=username,
                 password=password,
                 db=project)


@orca.injectable()
def bbox_dict() -> Dict[str, float]:
    return {'left': 9.0,
            'right': 9.1,
            'bottom': 54.5,
            'top': 54.6}


@orca.injectable()
def source_db() -> str:
    return 'europe'


@orca.injectable()
def recreate_db() -> bool:
    return False


@orca.injectable()
def source_srid() -> int:
    return 4326


@orca.injectable()
def target_srid() -> int:
    return 25832

