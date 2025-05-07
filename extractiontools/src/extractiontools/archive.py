import os
import logging
from subprocess import Popen, PIPE, STDOUT

from extractiontools.connection import DBApp


class Archive(DBApp):
    file_schema = '{database}.sql.gz'
    archive_folder = '/root/archive'

    def __init__(self, database: str, logger=None):
        super().__init__(logger=logger)
        self.fn = self.file_schema.format(database=database)
        self.logger = logger or logging.getLogger(self.__module__)
        self.set_login(database='postgres')
        self.database = database
        os.environ['PGSSLMODE'] = 'require'
        self.out_fp = os.path.join(self.archive_folder, self.fn)

    def archive(self):
        cmd = f'pg_dump -Fp "{self.database}" -h {self.login.host} -U {self.login.user} -p {self.login.port} '\
              f'| gzip > {self.out_fp}'
        self.logger.info(f'Archiviere {self.database}')
        self.logger.debug(cmd)
        process = Popen(cmd, stdout=PIPE, stderr=STDOUT, shell=True)
        # does nothing
        # with process.stdout:
        #     self.logger.info(process.stdout)
        exitcode = process.wait()
        if exitcode:
            raise IOError(f'Konnte {self.database} nicht archivieren.')
        else:
            self.logger.info(f'{self.database} nach {self.out_fp} archiviert.')

    def unarchive(self):
        if self.check_if_database_exists(self.database):
            raise Exception(f'Database {self.database} existiert bereits. Bitte vorher löschen.')
        os.environ['PGSSLMODE'] = 'require'
        self.create_target_db(self.database, 'group_osm')
        cmd = f'gunzip -c {self.out_fp} | psql -d {self.database} -h {self.login.host} -U {self.login.user} -p {self.login.port}'
        self.logger.info(f'Hole {self.database} aus dem Archiv')
        self.logger.debug(cmd)
        process = Popen(cmd, stdout=PIPE, stderr=STDOUT, shell=True)
        exitcode = process.wait()
        if exitcode:
            raise IOError(f'Fehler bei der Wiederherstellung.')
        else:
            self.logger.info(f'{self.database} aus {self.out_fp} wiederhergestellt.')

    def exists(self) -> bool:
        return os.path.exists(self.out_fp)

    def date_str(self) -> str:
        if not self.exists():
            return ''
        s = os.path.getmtime(self.out_fp)
        from datetime import datetime
        return datetime.fromtimestamp(s).strftime("%d.%m.%Y %I:%M:%S")