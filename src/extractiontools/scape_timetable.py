#!/usr/bin/env python
#coding:utf-8

from argparse import ArgumentParser

import numpy as np
import logging
logger = logging.getLogger()
logger.addHandler(logging.StreamHandler())
logger.level = logging.DEBUG
import os
import datetime
from extractiontools.connection import Connection, DBApp
from extractiontools.utils.get_date import Date

class ScrapeTimetable(DBApp):
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
                        dest="source_db", default='europe')
    parser.add_argument('--day', action="store", type=int,
                        help="day, default: day of today",
                        dest="day")
    parser.add_argument('--month', action="store", type=int,
                        help="month, default: month of today",
                        dest="month")
    parser.add_argument('--year', action="store", type=int,
                        help="year, default: year of today",
                        dest="year")

    options = parser.parse_args()
    date = Date(options.year, options.month, options.day)


    scrape = ScrapeTimetable()


