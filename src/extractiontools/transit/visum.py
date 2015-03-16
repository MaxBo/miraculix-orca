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
from simcommon.matrixio import XRecArray
import numpy as np

from extractiontools.utils.utf8csv import UnicodeWriter

from collections import OrderedDict
from extractiontools.transit.table import Table, Tables, Base


class DoubleComma(np.double):
    '''np.double for comma separated values'''
    def __new__(cls, val):
        if isinstance(val, (str, unicode)):
            val = val.replace(',', '.')
        return super(DoubleComma, cls).__new__(cls, val)

    def __repr__(self):
        """"""
        string = super(DoubleComma, self).__repr__().replace('.', ',')
        return string

    def __str__(self):
        return repr(self)

    def __unicode__(self):
        return unicode(repr(self))


class DoubleCommaLength(DoubleComma):
    '''np.double for comma separated values in km'''
    def __new__(cls, val):
        km = False
        if isinstance(val, (str, unicode)):
            if val.endswith('km'):
                km = True
            val = val.replace(',', '.').rstrip('km')
        self = super(DoubleCommaLength, cls).__new__(cls, val)
        if km:
            self *= 1000
        return self

    def __repr__(self):
        """"""
        km = self / 1000
        string = ('%0.3f' % (km)).replace('.', ',')
        return string + 'km'


class DoubleCommaTime(DoubleComma):
    '''np.double for comma separated values in min, sec or days'''
    def __new__(cls, val):
        factor = 1
        if isinstance(val, (str, unicode)):
            if val.endswith('s') or val.endswith('sec'):
                factor = 1
            elif val.endswith('m') or val.endswith('min'):
                factor = 60
            elif val.endswith('h') or val.endswith('hrs'):
                factor = 3600
            val = val.replace(',', '.').rstrip('smhecinr')
        self = super(DoubleCommaTime, cls).__new__(cls, val)
        if factor != 1:
            return self * factor
        return self

    def __repr__(self):
        """"""
        sec = self
        string = ('%d' % (sec)).replace('.', ',')
        return string + 's'


def coord_to_wgs84(from_proj, x, y, z='0'):
    wgs84 = Proj(proj='latlon', datum='WGS84')

    x = float(x.replace(',', '.'))
    y = float(y.replace(',', '.'))
    z = float(z.replace(',', '.'))

    return transform(from_proj, wgs84, x, y, z)

class HeaderException(Exception):
    def __init__(self, table):
        self.table = table


class Visum(Base):
    """Visum Netfile"""

    def __init__(self, netfile):
        super(Visum, self).__init__()
        self.netfile = netfile

    def add_tables(self):
        self.add_table(Version)
        self.add_table(Verkehrstag)
        self.add_table(Vsys)
        self.add_table(Knoten)
        self.add_table(Betreiber)
        self.add_table(Haltestelle)
        self.add_table(Haltestellenbereich)
        self.add_table(Haltepunkt)
        self.add_table(Linie)
        self.add_table(Linienroutenelement)
        self.add_table(Fahrzeitprofilelement)
        self.add_table(Fahrplanfahrt)
        self.add_table(Uebergangsgehzeithstber)

    @property
    def path(self):
        """The filepath"""
        return self.netfile

    def read_tables(self):
        """Read the tables from a net-file"""
        with codecs.open(self.netfile, encoding='latin-1') as handler:
            self.input_file = handler
            try:
                table_found = False
                while True:
                    current_line = self.next_line()

                    # current_line is a table header
                    if current_line.startswith('$'):
                        # finish the last block
                        if table_found:
                            table.read_file(header, lines)

                        # start with new block
                        header_line = current_line.strip().split(':')
                        tablename = header_line[0].lower().lstrip('$')

                        # check if real table with columns
                        if len(header_line) > 1:
                            header = header_line[1].split(';')
                            table_found = True

                        # get table class
                        table = self.get_table(tablename)
                        # if not relevant, skip table
                        if table is None:
                            table_found = False
                        # reset linies
                        lines = []

                    else:
                        # line in table body
                        lines.append(current_line.split(';'))

            except StopIteration:
                # finish the last block
                if table_found:
                    table.read_file(header, lines)
                print 'file completely read'

    def next_line(self):
        """Get the next line, that is not empty or a comment"""
        current_line = self.input_file.next().strip()
        if not current_line or current_line.startswith('*'):
            current_line = self.next_line()
        return current_line

    def write_tables(self):
        """write the tables"""
        with codecs.open(self.netfile, encoding='latin-1', mode='w') as f:
            header = '$VISION\n'
            f.writelines(header)
            for table in self._tables.itervalues():
                f.writelines(os.linesep)
                f.writelines('*{0}\n'.format(table.tablename))
                table.write_rows(f)




class VisumTable(Table):
    """Base Class for a visum table"""

    @property
    def marker(self):
        return '${name}'.format(name=self.__class__.__name__.upper())

    def open(self, mode):
        """"""

    def read_file(self, header, lines):
        """"""
        self.convert_lines(header, lines)

    @property
    def tablename(self):
        return self.__class__.__name__.upper()

    def write_file(self):
        """"""

    def write_rows(self, writer):
        """"""
        cols = ';'.join(cn for cn in self.cols)
        header = '${tn}:{cols}\n'.format(tn=self.tablename, cols=cols)
        writer.write(header)
        for row in self.rows:
            l = []
            vals = row.tolist()
            mask = row.mask.tolist()
            for c, colname in enumerate(self.cols):
                if mask[c]:
                    l.append('')
                else:
                    converter = self.cols.converters[c]
                    l.append(converter(vals[c]))
            line = ';'.join([unicode(x) for x in l])
            writer.write(line+os.linesep)


class Version(VisumTable):
    """Version"""
    def add_columns(self):
        self.add_column('VERSNR', DoubleComma, 9)
        self.add_column('FILETYPE', np.dtype('S50'), 'Net')
        self.add_column('LANGUAGE', np.dtype('S50'), 'DEU')


class Verkehrstag(VisumTable):
    """Verkehrstag"""
    def add_columns(self):
        self.add_column('NR', int)
        self.add_column('CODE', np.dtype('U50'))
        self.add_column('NAME', np.dtype('U50'))
        self.add_column('VTAGE', int)
        self.add_column('HFAKSTDKOST', DoubleComma)
        self.add_column('HFAKANGEBOT', DoubleComma)


class Vsys(VisumTable):
    """Verkehrssysteme"""
    def add_columns(self):
        self.add_column('CODE', np.dtype('U50'))
        self.add_column('NAME', np.dtype('U255'))
        self.add_column('TYP', np.dtype('U255'))


class Knoten(VisumTable):
    """Verkehrssysteme"""
    def add_columns(self):
        self.add_column('NR', np.int64)
        self.add_column('STEUERUNGSTYP', int)
        self.add_column('XKOORD', DoubleComma)
        self.add_column('YKOORD', DoubleComma)


class Betreiber(VisumTable):
    """Verkehrssysteme"""
    def add_columns(self):
        self.add_column('NR', np.int64)
        self.add_column('NAME', np.dtype('U255'))


class Haltestelle(VisumTable):
    """Verkehrssysteme"""
    def add_columns(self):
        self.add_column('NR', np.int64)
        self.add_column('CODE', np.dtype('U50'))
        self.add_column('NAME', np.dtype('U255'))
        self.add_column('XKOORD', DoubleComma)
        self.add_column('YKOORD', DoubleComma)


class Haltestellenbereich(VisumTable):
    """Verkehrssysteme"""
    def add_columns(self):
        self.add_column('NR', np.int64)
        self.add_column('HSTNR', np.int64)
        self.add_column('NAME', np.dtype('U255'))
        self.add_column('KNOTNR', np.int64)
        self.add_column('XKOORD', DoubleComma)
        self.add_column('YKOORD', DoubleComma)


class Haltepunkt(VisumTable):
    """Verkehrssysteme"""
    def add_columns(self):
        self.add_column('NR', np.int64)
        self.add_column('HSTBERNR', np.int64)
        self.add_column('NAME', np.dtype('U50'))
        self.add_column('NAME', np.dtype('U255'))
        self.add_column('GERICHTET', np.int8)
        self.add_column('KNOTNR', np.int64)
        self.add_column('VONKNOTNR', np.int64)
        self.add_column('STRNR', np.int64)
        self.add_column('RELPOS', DoubleComma)


class Linie(VisumTable):
    """Verkehrssysteme"""
    def add_columns(self):
        self.add_column('NAME', np.dtype('U255'))
        self.add_column('VSYSCODE', np.dtype('U50'))
        self.add_column('TARIFSYSTEMMENGE', np.dtype('U50'))
        self.add_column('BETREIBERNR', np.int64)


class Linienroutenelement(VisumTable):
    """Verkehrssysteme"""
    def add_columns(self):
        self.add_column('LINNAME', np.dtype('U255'))
        self.add_column('LINROUTENAME', np.dtype('U255'))
        self.add_column('RICHTUNGCODE', np.dtype('U1'))
        self.add_column('INDEX', np.int64)
        self.add_column('ISTROUTENPUNKT', np.int8)
        self.add_column('KNOTNR', np.int64)
        self.add_column('HPUNKTNR', np.int64)
        self.add_column('NACHLAENGE', DoubleCommaLength)


class Fahrzeitprofilelement(VisumTable):
    """Verkehrssysteme"""
    def add_columns(self):
        self.add_column('LINNAME', np.dtype('U255'))
        self.add_column('LINROUTENAME', np.dtype('U255'))
        self.add_column('RICHTUNGCODE', np.dtype('U1'))
        self.add_column('FZPROFILNAME', np.dtype('U255'))
        self.add_column('INDEX', np.int64)
        self.add_column('LRELEMINDEX', np.int64)
        self.add_column('AUS', np.int8)
        self.add_column('EIN', np.int8)
        self.add_column('ANKUNFT', np.dtype('S8'))
        self.add_column('ABFAHRT', np.dtype('S8'))


class Fahrplanfahrt(VisumTable):
    """Verkehrssysteme"""
    def add_columns(self):
        self.add_column('NR', np.int64)
        self.add_column('NAME', np.dtype('U255'))
        self.add_column('ABFAHRT', np.dtype('S8'))
        self.add_column('LINNAME', np.dtype('U255'))
        self.add_column('LINROUTENAME', np.dtype('U255'))
        self.add_column('RICHTUNGCODE', np.dtype('U1'))
        self.add_column('FZPROFILNAME', np.dtype('U255'))
        self.add_column('VONFZPELEMINDEX', np.int64)
        self.add_column('NACHFZPELEMINDEX', np.int64)
        self.add_column('BETREIBERNR', np.int64)


class Uebergangsgehzeithstber(VisumTable):
    """Verkehrssysteme"""
    def add_columns(self):
        self.add_column('VONHSTBERNR', np.int64)
        self.add_column('NACHHSTBERNR', np.int64)
        self.add_column('VSYSCODE', np.dtype('U50'))
        self.add_column('ZEIT', DoubleCommaTime)
