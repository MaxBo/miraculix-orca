import os
import pandas as pd
from extractiontools.connection import Connection, DBApp, Login


class ImportPendlerdaten(DBApp):
    """
    Import Commuter trips from excel-files to database
    """
    schema = 'pendler'
    role = 'group_osm'

    def __init__(self,
                 db: str = 'extract',
                 subfolder: str = 'pendler', **kwargs):
        super().__init__(schema=self.schema, **kwargs)
        self.destination_db = self.db = db
        self.set_login(database=db)
        self.check_platform()
        self.subfolder = subfolder

    def run(self):
        """
        """
        with Connection(login=self.login) as conn:
            # preparation
            self.conn = conn
            self.import_pendler()
            self.conn.commit()

    def import_pendler(self):
        """import Pendlerdaten"""
        path = os.path.join(self.folder, self.subfolder)
        files = os.listdir(path)
        for file in files:
            self.process_file(file)

    def process_file(self, file: str):
        """upload a single excel-file with Pendlerdaten"""
