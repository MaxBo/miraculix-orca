#!/usr/bin/env python
#coding:utf-8

import numpy as np
import logging
logger = logging.getLogger()
logger.addHandler(logging.StreamHandler())
logger.level = logging.DEBUG
import os
import datetime
from extractiontools.connection import Connection, DBApp


class ScrapeStops(DBApp):
    def __init__(self, schema='schleswig_flensburg',
                 day=25, month=11, year=2014):
        self.schema = schema
        self.today = datetime.datetime(year, month, day)



if __name__=='__main__':


    parser = ArgumentParser(description="Scrape Stops in a given bounding box")

    parser.add_argument("-t", '--top', action="store",
                        help="top", type=float,
                        dest="top", default=54.65)
    parser.add_argument("-b", '--bottom,', action="store",
                        help="bottom", type=float,
                        dest="bottom", default=54.6)
    parser.add_argument("-r", '--right', action="store",
                        help="right", type=float,
                        dest="right", default=10.0)
    parser.add_argument("-l", '--left', action="store",
                        help="left", type=float,
                        dest="left", default=9.95)

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
                        dest="source_db", default='dplus')

    bbox = BBox(top=options.top, bottom=options.bottom,
                left=options.left, right=options.right)
    extract = ExtractOSM(source_db=options.source_db,
                         destination_db=options.destination_db,
                         target_srid=options.srid,
                         recreate_db=False)
    extract.set_login(host=options.host, port=options.port, user=options.user)

    options = parser.parse_args()
