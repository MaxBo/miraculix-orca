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
        for tn, geom in self.tables.iteritems():
            self.add_index(tn, geom, pkey='ogc_fid')

    def add_index(self, tn, geom, pkey):
        sql = """
ALTER TABLE {sn}.{tn} ADD PRIMARY KEY ({pkey});
--CREATE INDEX {tn}_geom_key ON {sn}.{tn} USING btree({pkey});
CREATE INDEX {tn}_geom_idx ON {sn}.{tn} USING gist({geom});
        """.format(sn=self.schema, tn=tn, geom=geom, pkey=pkey)
        self.run_query(sql, conn=self.conn1)


if __name__ == '__main__':

    parser = ArgumentParser(description="Extract Data for Model")

    parser.add_argument("-n", '--name', action="store",
                        help="Name of destination database",
                        dest="destination_db", default='extract')

    parser.add_argument('--host', action="store",
                        help="host",
                        dest="host", default='gis.ggr-planung.de')
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
                        dest='tabellen', default=['gem_2015_01'])


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
