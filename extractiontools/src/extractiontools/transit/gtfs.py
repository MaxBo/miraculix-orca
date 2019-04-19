#!/usr/bin/env python
#coding:utf-8
# Author:   --<>
# Purpose:
# Created: 13/03/2015

from abc import ABCMeta, abstractproperty, abstractmethod
import os
import sys
import codecs
import csv
import numpy as np

from extractiontools.utils.utf8csv import UnicodeWriter, UnicodeReader
from extractiontools.utils.file_in_zipfile import (ReadFileInZipfile,
                                                   WriteFileInZipfile)
from extractiontools.transit.table import Table, Base, logger

__all__ = ('GTFS', 'GTFSTable',
           'Agency', 'Calendar', 'Trips', 'Stops', 'StopTimes',
           'Routes', 'Transfers')


class Date_S8(np.datetime64):
    '''np.datetime64 for values'''
    unit = 'D'

    def __new__(cls, val):
        if isinstance(val, (str, unicode)):
            if len(val) == 8:
                val = '{Y}-{M}-{D}'.format(Y=val[:4], M=val[4:6], D=val[6:])
            elif len(val) == 6:
                val = '{Y}-{M}'.format(Y=val[:4], M=val[4:6])
            else:
                val = val.zfill(4)
        self = super(Date_S8, cls).__new__(cls, val)
        return self

    def __repr__(self):
        """"""
        string = self.astype('M8[D]').__str__().replace('-', '')
        return string

    def __str__(self):
        return self.__repr__()

    def __unicode__(self):
        return self.__repr__()


class GTFS(Base):
    """gtfs zipfile"""

    def __init__(self, folder, filename='gtfs.zip'):
        super(GTFS, self).__init__()
        self.folder = folder
        self.filename = filename

    def add_tables(self):
        self.add_table(Calendar)
        self.add_table(Agency)
        self.add_table(Stops)
        self.add_table(StopTimes)
        self.add_table(Routes)
        self.add_table(Trips)
        self.add_table(Transfers)
        self.add_table(Shapes)

    @property
    def path(self):
        """The filepath"""
        return os.path.join(self.folder, self.filename)

    def read_tables(self):
        """read all tables"""
        for table in self._tables.itervalues():
            table.read_file()

    def write_tables(self):
        """write the tables"""
        if os.path.exists(self.path):
            os.remove(self.path)
        for table in self._tables.itervalues():
            table.write_file()


class GTFSTable(Table):
    """Base Class for a gtfs file"""
    __metaclass__ = ABCMeta

    def open(self, mode='a'):
        zipfilepath = self.tables.path
        if 'r' in mode:
            return ReadFileInZipfile(zipfilepath, mode, self.tablename)
        elif mode in 'aw':
            return WriteFileInZipfile(zipfilepath, mode, self.tablename)
        else:
            raise AttributeError('mode must be r, a or w')

    def read_file(self):
        """method to read data from a zipfile"""
        with self.open(mode='r') as f:
            reader = UnicodeReader(f, delimiter=self.sep,
                                   quotechar='"',
                                   quoting=csv.QUOTE_MINIMAL)
            header = reader.next()
            lines = [line for line in reader if line]
            self.convert_lines(header, lines)

    def write_file(self):
        """method to write the file"""
        with self.open() as f:
            writer = UnicodeWriter(f, delimiter=self.sep,
                                   quotechar='"',
                                   quoting=csv.QUOTE_MINIMAL)
            writer.writerow(self.header)
            self.write_rows(writer)

    def write_rows(self, writer):
        """write the rows"""
        for row in self.rows:
            line = self.sep.join([unicode(c) for c in row.tolist()])
            writer.writerow(row.tolist())


class Shapes(GTFSTable):
    """"""
    @property
    def tablename(self):
        return 'shapes.txt'

    def add_columns(self):
        self.add_column('shape_id', int)
        self.add_column('shape_pt_lat', np.double)
        self.add_column('shape_pt_lon', np.double)
        self.add_column('shape_pt_sequence', int)

        self.add_pkey('shape_id')


class StopTimes(GTFSTable):
    """"""
    @property
    def tablename(self):
        return 'stop_times.txt'

    def add_columns(self):
        self.add_column('trip_id', int)
        self.add_column('arrival_time', np.dtype('S8'))
        self.add_column('departure_time', np.dtype('S8'))
        self.add_column('stop_id', np.dtype('U50'))
        self.add_column('stop_sequence', int)
        self.add_column('pickup_type', int, 0)
        self.add_column('drop_off_type', int, 0)

        self.add_pkey('trip_id', 'stop_sequence')


class Trips(GTFSTable):
    """"""
    @property
    def tablename(self):
        return 'trips.txt'

    def add_columns(self):
        self.add_column('route_id', np.dtype('U50'))
        self.add_column('service_id', int, 1)
        self.add_column('trip_id', int)
        self.add_column('shape_id', int)


class Transfers(GTFSTable):
    """"""
    @property
    def tablename(self):
        return 'transfers.txt'

    def add_columns(self):
        self.add_column('from_stop_id', np.dtype('U50'))
        self.add_column('to_stop_id', np.dtype('U50'))
        self.add_column('transfer_type', int, 2)
        self.add_column('min_transfer_time', np.float32)


class Routes(GTFSTable):
    """"""
    @property
    def tablename(self):
        return 'routes.txt'

    def add_columns(self):
        self.add_column('route_id', np.dtype('U255'))
        self.add_column('agency_id', np.dtype('U50'))
        self.add_column('route_short_name', np.dtype('U50'))
        self.add_column('route_long_name', np.dtype('U255'))
        self.add_column('route_type', int)


class Stops(GTFSTable):
    """"""
    @property
    def tablename(self):
        return 'stops.txt'

    def add_columns(self):
        self.add_column('stop_id', np.dtype('U50'))
        self.add_column('stop_name', np.dtype('U255'))
        self.add_column('stop_lat', np.dtype('f8'))
        self.add_column('stop_lon', np.dtype('f8'))
        self.add_column('location_type', int, 0)
        self.add_column('parent_station', np.dtype('U50'), '')

        self.add_pkey('stop_id')


class Agency(GTFSTable):
    """"""
    @property
    def tablename(self):
        return 'agency.txt'

    def add_columns(self):
        self.add_column('agency_id', int)
        self.add_column('agency_name', np.dtype('U255'))
        self.add_column('agency_url', np.dtype('U255'), 'www.example.com')
        self.add_column('agency_timezone', np.dtype('U255'), u'Europe/Berlin')


class Calendar(GTFSTable):
    """"""
    @property
    def tablename(self):
        return 'calendar.txt'

    def add_columns(self):
        self.add_column('service_id', int, 1)
        self.add_column('monday', int, 1)
        self.add_column('tuesday', int, 1)
        self.add_column('wednesday', int, 1)
        self.add_column('thursday', int, 1)
        self.add_column('friday', int, 1)
        self.add_column('saturday', int, 1)
        self.add_column('sunday', int, 1)
        self.add_column('start_date', np.dtype('S8'), '20000101')
        self.add_column('end_date', np.dtype('S8'), '20201231')
        #self.add_column('start_date', Date_S8, Date_S8('20000101'))
        #self.add_column('end_date', Date_S8, Date_S8('20201231'))

    def calc_start_date(self):
        """Set start dates as datetime"""
        row = self.rows[0].start_date
        val = '{Y}-{M}-{D}'.format(Y=row[:4], M=row[4:6], D=row[6:])
        self._start_date = np.datetime64(val)


def main():
    pass

if __name__ == '__main__':
    main()