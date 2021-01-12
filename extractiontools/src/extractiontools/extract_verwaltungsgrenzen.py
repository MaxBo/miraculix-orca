#!/usr/bin/env python
#coding:utf-8

from argparse import ArgumentParser

from extractiontools.connection import Connection
from extractiontools.ausschnitt import Extract


class ExtractVerwaltungsgrenzen(Extract):
    """
    Extract the osm data
    """
    tables = {}
    schema = 'verwaltungsgrenzen'
    role = 'group_osm'

    def final_stuff(self):
        # foreign tables seem not to pass the pkeys
        # so the pkeys are retrieved from the foreign server directly
        with Connection(login=self.foreign_login) as conn:
            for tn, geom in self.tables.items():
                self.add_geom_index(tn, geom)
                pkey = self.get_primary_key(self.schema, tn, conn=conn)
                if pkey:
                    self.add_pkey(tn, pkey)
                else:
                    self.add_pkey(tn, 'ags')

    def add_pkey(self, tn, pkey):
        sql = """
        ALTER TABLE {sn}.{tn} ADD PRIMARY KEY ({pkey});
        """.format(sn=self.schema, tn=tn, pkey=pkey)
        self.run_query(sql, conn=self.conn)

    def add_geom_index(self, tn, geom):
        sql = """
        CREATE INDEX {tn}_geom_idx ON {sn}.{tn} USING gist({geom});
        """.format(sn=self.schema, tn=tn, geom=geom)
        self.run_query(sql, conn=self.conn)


if __name__ == '__main__':

    parser = ArgumentParser(description="Extract Data for Model")

    parser.add_argument("-n", '--name', action="store",
                        help="Name of destination database",
                        dest="destination_db", default='extract')

    parser.add_argument('--host', action="store",
                        help="host",
                        dest="host", default='localhost')
    parser.add_argument("-p", '--port', action="store",
                        help="port", type=int,
                        dest="port", default=5432)

    parser.add_argument("-U", '--user', action="store",
                        help="database user",
                        dest="user", default='osm')
    parser.add_argument('--source-db', action="store",
                        help="source database",
                        dest="source_db", default='europe')

    parser.add_argument('--tabellen', action='store',
                        help='tabellen to extract', nargs='*',
                        dest='tabellen', default=['gem_2015_01',
                                                  'vwg_2015_01',
                                                  'krs_2015_01',
                                                  'lan_2015_01',
                                                  'gem_2014_ew_svb',
                                                  'plz_2016'])


    options = parser.parse_args()

    extract = ExtractVerwaltungsgrenzen(source_db=options.source_db,
                         destination_db=options.destination_db)
    extract.tables = dict([(f, 'geom') for f in options.tabellen])

    extract.set_login(host=options.host,
                      port=options.port,
                      user=options.user)
    extract.get_target_boundary()
    extract.get_target_srid()
    extract.extract()
