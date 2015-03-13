#!/usr/bin/env python
#coding:utf-8
# Author:   --<>
# Purpose:
# Created: 13/03/2015

from abc import ABCMeta, abstractproperty, abstractmethod
from zipfile import ZipFile
import os
import sys
import codecs
import csv
from pyproj import Proj, transform

from extractiontools.utils.utf8csv import UnicodeWriter


def to_float(val):
    return float(val.replace(',', '.'))

def coord_to_wgs84(from_proj, x, y, z='0'):
    wgs84 = Proj(proj='latlon', datum='WGS84')

    x = float(x.replace(',', '.'))
    y = float(y.replace(',', '.'))
    z = float(z.replace(',', '.'))

    return transform(from_proj, wgs84, x, y, z)

class HeaderException(Exception):
    def __init__(self, table):
        self.table = table


class Column(object):
    def __init__(self, name, dtype):
        self.name = name
        self.dtype = dtype


class Row(object):
    __metaclass__ = ABCMeta

    def __init__(self, line, rows):
        """Row"""
        self._entries = line.strip().split(';')
        self.rows = rows

    def __getattr__(self, name):
        return self.rows.get_value(name, self)

    def __repr__(self):
        cols = (str(self.__getattr__(name))
                       for name in self.rows.cols)
        vals = ';'.join(cols)
        return '{name}: {vals}'.format(name=self.rows.table, vals=vals)


class Table(object):
    __metaclass__ = ABCMeta
    def __init__(self, header_row):
        table, header = header_row.strip().replace('-', '_').split(':')
        self.table = table
        self.cols = header.strip().split(';')
        self.col_idx = dict(zip(self.cols, range(len(self.cols))))
        self.rows = {}
        self.col_formatter = {}
        self.add_col_formats()

    def add_col_formats(self):
        pass

    @abstractproperty
    def pkey(self):
        """The primary key of the table"""

    def get_pkey(self, row):
        return getattr(row, self.pkey)

    def get_value(self, name, row):
        idx = self.col_idx[name]
        val = row._entries[idx]
        return self.format_col(name, val)

    def format_col(self, name, val):
        if name in self.col_formatter:
            func = self.col_formatter[name]
            return func(val)
        return val

    def add_row(self, line):
        row = Row(line, self)
        pkey = self.get_pkey(row)
        self.rows[pkey] = row

    def get_row(self, pkey):
        return self.rows[pkey]

    def get_latlon(self, pkey):
        row = self.get_row(from_proj, pkey)
        lat, lon, h = coord_to_wgs84(from_proj, row.XKOORD, row.YKOORD)


class Vertices(Table):
    @property
    def pkey(self):
        return 'NR'

    def add_col_formats(self):
        self.col_formatter['NR'] = int
        self.col_formatter['XKOORD'] = to_float
        self.col_formatter['YKOORD'] = to_float


class Stops(Table):
    @property
    def pkey(self):
        return 'NR'

    def add_col_formats(self):
        self.col_formatter['NR'] = int
        self.col_formatter['XKOORD'] = to_float
        self.col_formatter['YKOORD'] = to_float


class StopArea(Table):
    @property
    def pkey(self):
        return 'NR'

    def add_col_formats(self):
        self.col_formatter['NR'] = int
        self.col_formatter['HSTNR'] = int
        self.col_formatter['KNOTNR'] = int
        self.col_formatter['XKOORD'] = to_float
        self.col_formatter['YKOORD'] = to_float

class StopArea(Table):
    @property
    def pkey(self):
        return 'NR'

    def add_col_formats(self):
        self.col_formatter['NR'] = int
        self.col_formatter['HSTNR'] = int
        self.col_formatter['KNOTNR'] = int
        self.col_formatter['XKOORD'] = to_float
        self.col_formatter['YKOORD'] = to_float



class GTFS(object):
    def __init__(self):
        self.vertices = {}
        self.shapes = {}


class Base(object):
    """Base Class"""
    __metaclass__ = ABCMeta

    def __init__(self, gtfs, in_handler, outfile):
        self.gtfs = gtfs
        self.handler = in_handler
        self.outfile = outfile

    @abstractproperty
    def tablename(self):
        """the tablename"""

    @abstractproperty
    def header(self):
        """The file header of the output file"""

    def write_file(self):
        """method to write the file"""
        with open(self.tablename, 'w') as f:
            writer = UnicodeWriter(f, delimiter=',',
                                   quotechar='"',
                                   quoting=csv.QUOTE_MINIMAL)
            writer.writerow(self.header)
            self.write_body(writer)

    @abstractmethod
    def write_body(self, writer):
        """write the body"""

    def write_to_zipfile(self):
        """append file to zipfile and remove tempfile"""
        try:
            zip_file = ZipFile(self.output_file, 'a')
            zip_file.write(self.tablename)
        finally:
            zip_file.close()

        os.remove(self.tablename)



class Shape(Base):
    """"""
    @property
    def tablename(self):
        return 'shapes.txt'

    @property
    def header(self):
        return ( u'shape_id', u'shape_pt_lat',
                 u'shape_pt_lon', u'shape_pt_sequence' )

    def write_body(gtfs, writer):
        gtfs = self.gtfs
        for s_id in gtfs.shapes:
            for entry in gtfs.shapes[s_id]:
                s_seq = entry[0]
                lat, lon  = gtfs.vertices[entry[1]]

                writer.writerow(( s_id, lat, lon, s_seq ))


def main():
    line = '408;0;1043195,4640315624;7310729,6538941395;0;;26012977'
    v = Vertices('$KNOTEN:NR;STEUERUNGSTYP;XKOORD;YKOORD;ZWERT1;HST-KAT;OSM_NODE_ID')
    v.add_row(line)
    row = v.get_row(408)
    print row

if __name__ == '__main__':
    main()