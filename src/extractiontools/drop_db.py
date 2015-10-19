#!/usr/bin/env python
#coding:utf-8

from argparse import ArgumentParser

from extractiontools.ausschnitt import logger, Extract
from extractiontools.connection import Connection

class DropDatabase(Extract):
    """Drop Database"""
    def extract(self):
        self.set_pg_path()
        exists = self.check_if_database_exists(self.destination_db)
        if not exists:
            msg = 'database {} does not exist'
            logger.info(msg.format(self.destination_db))
            return
        with Connection(login=self.login0) as conn:
            cursor = conn.cursor()
            sql = """
SELECT can_be_deleted FROM meta_master.projekte WHERE projektname_kurz = '{}';
            """.format(self.destination_db)
            cursor.execute(sql)
            row = cursor.fetchone()
            if row is None:
                msg = 'database {} not in project table'
                logger.info(msg.format(self.destination_db))
                return
            if not row.can_be_deleted:
                msg = 'database {} is protected and cannot be deleted'
                logger.info(msg.format(self.destination_db))
                return
            self.drop_database(dbname=self.destination_db, conn=conn)
            sql = """
UPDATE meta_master.projekte SET deleted = True WHERE projektname_kurz = '{}';"""
            cursor.execute(sql.format(self.destination_db))
            conn.commit()
            msg = 'database {} successfully deleted'
            logger.info(msg.format(self.destination_db))


if __name__ == '__main__':

    parser = ArgumentParser(description="Drop Database")

    parser.add_argument("-n", '--name', action="store",
                        help="Name of destination database",
                        dest="destination_db", default='extract')

    parser.add_argument('--host', action="store",
                        help="host",
                        dest="host", default='gis.ggr-planung.de')
    parser.add_argument("-p", '--port', action="store",
                        help="port", type=int,
                        dest="port", default=5432)

    parser.add_argument('--recreate', action="store_true",
                        help="recreate",
                        dest="recreate", default=False)

    parser.add_argument("-U", '--user', action="store",
                        help="database user",
                        dest="user", default='osm')
    parser.add_argument('--source-db', action="store",
                        help="source database",
                        dest="source_db", default='dplus')

    options = parser.parse_args()

    extract = DropDatabase(source_db=options.source_db,
                           destination_db=options.destination_db,)
    extract.set_login(host=options.host, port=options.port, user=options.user)
    extract.extract()
