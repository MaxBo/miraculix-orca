#!/usr/bin/env python
# coding:utf-8

import numpy as np
import logging
logger = logging.getLogger('OrcaLog')
import os
import datetime
from extractiontools.connection import Connection, DBApp


class GTFS2Visum(DBApp):
    def __init__(self, schema='schleswig_flensburg',
                 day=25, month=11, year=2014):
        self.schema = schema
        self.today = datetime.datetime(year, month, day)

    def convert(self):
        with Connection() as conn:
            self.conn = conn
            self.set_search_path()

            self.conn.commit()
            pass


if __name__ == '__main__':

    parser = ArgumentParser(description="Convert GTFS to Visum NetFile")

    options = parser.parse_args()
