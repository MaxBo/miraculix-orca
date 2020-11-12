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
#from simcommon.matrixio import XRecArray
import numpy as np

from extractiontools.utils.utf8csv import UnicodeWriter

from collections import OrderedDict
class Columns(OrderedDict):
    """"""
    @property
    def dtype(self):
        dtype = {'names': self.keys(),
                'formats': self.values(),}
        return dtype


class Column(object):
    """a column"""
    def __init__(self, name, dtype):
        self.name = name
        self.dtype = dtype


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

    def pkey(self):
        """The primary key of the table"""
        return ('NR', )

    def get_pkey(self, row):
        return tuple(getattr(row, col) for col in self.pkey())

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


class KNOTEN(Table):

    def add_col_formats(self):
        self.col_formatter['NR'] = int
        self.col_formatter['XKOORD'] = to_float
        self.col_formatter['YKOORD'] = to_float


class BETREIBER(Table):
    """"""

class HALTESTELLE(Table):
    def add_col_formats(self):
        self.col_formatter['NR'] = int
        self.col_formatter['XKOORD'] = to_float
        self.col_formatter['YKOORD'] = to_float


class LINIE(Table):
    def add_col_formats(self):
        self.col_formatter['BETREIBERNR'] = int


class HALTESTLLENBEREICH(Table):
    def add_col_formats(self):
        self.col_formatter['NR'] = int
        self.col_formatter['HSTNR'] = int
        self.col_formatter['KNOTNR'] = int
        self.col_formatter['XKOORD'] = to_float
        self.col_formatter['YKOORD'] = to_float


class UEBERGANGSGEHZEITHSTBER(Table):
    @property
    def pkey(self):
        return ('VONHSTBERNR', 'NACHHSTBERNR')

    def add_col_formats(self):
        self.col_formatter['VONHSTBERNR'] = int
        self.col_formatter['NACHHSTBERNR'] = int
        self.col_formatter['KNOTNR'] = int
        self.col_formatter['XKOORD'] = to_float
        self.col_formatter['YKOORD'] = to_float


class FAHRPLANFAHRT(Table):
    def add_col_formats(self):
        self.col_formatter['VONFZPELEMINDEX'] = int
        self.col_formatter['NACHFZPELEMINDEX'] = int
        self.col_formatter['BETREIBERNR'] = int


class LINIENROUTENELEMENT(Table):
    @property
    def pkey(self):
        return ('LINNAME', 'LINROUTENAME' , 'RICHTUNGCODE', 'INDEX')

    def add_col_formats(self):
        self.col_formatter['NR'] = int
        self.col_formatter['HSTNR'] = int
        self.col_formatter['KNOTNR'] = int
        self.col_formatter['XKOORD'] = to_float
        self.col_formatter['YKOORD'] = to_float


class HALTEPUNKT(Table):

    def add_col_formats(self):
        self.col_formatter['NR'] = int
        self.col_formatter['HSTBERNR'] = int
        self.col_formatter['KNOTNR'] = int
        self.col_formatter['VONKNOTNR'] = int
        self.col_formatter['STRNR'] = int
        self.col_formatter['RELPOS'] = to_float


class GTFS(object):
    def __init__(self, infile):
        """"""
        self.infile = infile

    def read(self):

        with codecs.open(self.net_file, encoding='latin-1') as hander:
            self.handler = handler
            try:
                while True:
                    current_line = self.get_line()

                    # current_line is a table header
                    if current_line.startswith('$'):
                        table_name = current_line.split(':')[0]
                        if table_name in self.table_to_func_mapper:
                            current_line = self.table_to_func_mapper[table_name](current_line)
                        else:
                            current_line = self.get_line()
                    else:
                        current_line = self.get_line()

            except HeaderException as err:
                table = err.table

            except StopIteration:
                self._write_shapes()
                if self.debug: print( 'file completely read')

            #except:
            #   if self.debug: print 'error in write_gtf'
            #  raise InvalidInputException()
            #finally:
                #self.input_file.close()

        def get_line(self):
            current_line = self.input_file.next().strip()
            if not current_line or current_line.startswith('*'):
                current_line = self.get_line()
            return current_line


class GTFSTable(object):
    """Base Class for a gtfs file"""
    __metaclass__ = ABCMeta

    def __init__(self, n_rows, folder, sep=','):
        self.folder= folder
        self.sep = sep
        self.cols = Columns()
        self.add_columns()
        self.add_rows()

    @abstractproperty
    def tablename(self):
        """the tablename"""

    def header(self):
        """The file header of the output file"""
        return ','.join(self.columns.iterkeys())

    def write_file(self):
        """method to write the file"""
        with open(self.tablename, 'w') as f:
            writer = UnicodeWriter(f, delimiter=',',
                                   quotechar='"',
                                   quoting=csv.QUOTE_MINIMAL)
            writer.writerow(self.header)
            self.write_body(writer)

    def add_column(self, name, dtype):
        """add a column"""
        self.cols[name] = dtype

    @abstractmethod
    def add_columns(self):
        """add the columns"""

    def add_row(self, row):
        """add a row"""
        self.rows.append(row)

    def add_rows(self, data):
        self.rows = np.recarray.empty(self.n_rows, dtype=self.cols.dtype)

    def write_rows(self, writer):
        """write the rows"""
        for row in self.rows():
            line = self.sep.join(row)
            writer.write_row(line)

    @abstractmethod
    def parse_visum(self, visum_tables):
        """parse visum data"""

    def write_to_zipfile(self):
        """append file to zipfile and remove tempfile"""
        try:
            zip_file = ZipFile(self.output_file, 'a')
            zip_file.write(self.tablename)
        finally:
            zip_file.close()

        os.remove(self.tablename)


class GTFSfromVISUM(object):
    """MixinClass to create GTFS rom VISUM"""
    ___metaclass__ = ABCMeta

    @classmethod
    def from_visum(cls, visum, folder):
        n_rows = self.get_n_rows(visum)
        self = cls(n_rows, folder)
        self.parse_data(visum)
        return self

    @abstractproperty
    def visum_tables(self):
        """The relevant visum tables"""

    def get_n_rows(self, visum):
        """get the number of rows for the destination table"""
        main_visum_table = getattr(visum, self.visum_tables[0])
        return main_visum_table.n_rows

    @abstractmethod
    def parse_data(self, visum):
        """parse the visum data"""


class Shape(GTFS):
    """"""
    @property
    def tablename(self):
        return 'shapes.txt'


    def add_columns(self):
        self.add_column(u'shape_id', np.int)
        self.add_column(u'shape_pt_lat', np.double)
        self.add_column(u'shape_pt_lon', np.double)
        self.add_column(u'shape_pt_sequence', np.int)


class ShapeFromVisum(Shape, GTFSfromVISUM):
    """Create """
    @property
    def visum_tables(self):
        return 'LINIENROUTENELEMENT'

    def parse_data(self, visum):
        gtfs = self.gtfs
        for s_id in gtfs.shapes:
            for entry in gtfs.shapes[s_id]:
                s_seq = entry[0]
                lat, lon  = gtfs.vertices[entry[1]]

                writer.writerow(( s_id, lat, lon, s_seq ))


def main():
    line = '408;0;1043195,4640315624;7310729,6538941395;0;;26012977'
    v = KNOTEN('$KNOTEN:NR;STEUERUNGSTYP;XKOORD;YKOORD;ZWERT1;HST-KAT;OSM_NODE_ID')
    v.add_row(line)
    row = v.get_row(408)
    print(row)

if __name__ == '__main__':
    main()