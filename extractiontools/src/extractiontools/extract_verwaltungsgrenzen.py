#!/usr/bin/env python
#coding:utf-8

from argparse import ArgumentParser

from extractiontools.ausschnitt import Extract, BBox, logger


class ExtractVerwaltungsgrenzen(Extract):
    """
    Extract the osm data
    """
    tables = {}
    schema = 'verwaltungsgrenzen'
    role = 'group_osm'

    def final_stuff(self):
        for tn, geom in self.tables.items():
            self.add_geom_index(tn, geom)
            pkey = self.get_primary_key(self.schema, tn, conn=self.conn0)
            if pkey:
                self.add_pkey(tn, pkey)
        self.add_pkey('gem_2014_ew_svb', 'ags')

    def add_pkey(self, tn, pkey):
        sql = """
ALTER TABLE {sn}.{tn} ADD PRIMARY KEY ({pkey});
        """.format(sn=self.schema, tn=tn, pkey=pkey)
        self.run_query(sql, conn=self.conn1)

    def add_geom_index(self, tn, geom):
        sql = """
CREATE INDEX {tn}_geom_idx ON {sn}.{tn} USING gist({geom});
        """.format(sn=self.schema, tn=tn, geom=geom)
        self.run_query(sql, conn=self.conn1)


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
                         destination_db=options.destination_db,
                         recreate_db=False)
    extract.tables = dict([(f, 'geom') for f in options.tabellen])

    extract.set_login(host=options.host,
                      port=options.port,
                      user=options.user)
    extract.get_target_boundary_from_dest_db()
    extract.get_target_srid_from_dest_db()
    extract.extract()
