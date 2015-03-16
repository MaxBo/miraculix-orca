#!/usr/bin/env python
#coding:utf-8
# Author:   --<>
# Purpose:
# Created: 13/03/2015

from abc import ABCMeta, abstractproperty, abstractmethod
from simcommon.matrixio import XRecArray
from collections import OrderedDict
import os
import numpy as np
from simcommon.matrixio import XMaskedRecarray


class Base(object):
    """The base class for projects"""
    __metaclass__ = ABCMeta

    def __init__(self):
        """Init to specify in the subclass"""
        self._tables = Tables()
        self.add_tables()

    def add_table(self, TableClass):
        """Add a table"""
        name = TableClass.__name__.lower()
        self._tables[name] = TableClass(self)

    @abstractmethod
    def add_tables(self):
        """To define in the subclass"""

    def get_table(self, name):
        return self._tables.get(name, None)

    def __getattr__(self, name):
        if name in self._tables:
            return self.get_table(name)
        super(Base, self).__getattribute__(name)

    @abstractproperty
    def path(self):
        """The path"""

    def __repr__(self):
        return '{cls}-Object at {path}'.format(cls=self.__class__.__name__,
                                               path=self.path)

    def __str__(self):
        lines = [repr(self)]
        for table in self._tables.itervalues():
            lines.append(table.__repr__())
        return os.linesep.join(lines)

    @abstractmethod
    def read_tables(self):
        """read all tables"""


class Tables(OrderedDict):
    """the tables"""


class Columns(OrderedDict):
    """"""
    @property
    def dtype(self):
        dtype = {'names': self.keys(),
                'formats': self.values(),}
        return dtype

    @property
    def converters(self):
        try:
            return self._converters
        except AttributeError:
            self.update_converters()
            return self._converters

    def update_converters(self):
        self._converters = self.get_converters()

    def get_converters(self):
        converters = []
        for dt in self.dtype['formats']:
            if isinstance(dt, type):
                dtype = dt
            else:
                dtype = dt.type
            converters.append(dtype)

        return converters

    def convert_row(self, row):
        return [self.converters[i](col)
                if col != ''
                else self.converters[i]('0')
                for i, col in enumerate(row)]

    def get_column_index(self, header):
        """Check if header is ok"""
        col_index = []
        for i, col in enumerate(self):
            try:
                c = header.index(col)
            except ValueError:
                c = -1
            col_index.append(c)
        return col_index


class Table(object):
    """Base Class for a table"""
    __metaclass__ = ABCMeta

    def __init__(self, tables, sep=','):
        self.tables= tables
        self.sep = sep
        self.cols = Columns()
        self.defaults = OrderedDict()
        self.add_columns()
        self.cols.update_converters()
        self.add_rows(0)

    @abstractmethod
    def open(self, mode):
        """
        open a table
        """

    def __repr__(self):
        """A nice representation"""
        return os.linesep.join((self.tablename, self.rows.__repr__()))

    @abstractproperty
    def tablename(self):
        """the tablename"""

    @property
    def n_rows(self):
        return self.rows.n_rows

    @property
    def header(self):
        """The file header of the output file"""
        return self.cols.keys()

    @abstractmethod
    def write_file(self):
        """method to write the file"""

    @abstractmethod
    def read_file(self):
        """method to read the file"""

    def add_column(self, name, dtype, default=None):
        """add a column"""
        try:
            if isinstance(dtype, np.dtype):
                converter = dtype.type
            else:
                converter = dtype
            r = converter('0')
        except TypeError:
            msg = '{dtype} cannot convert string to numpy.dtype'
            raise
            #raise TypeError, msg.format(dtype=dtype)
        self.cols[name] = dtype
        self.defaults[name] = default

    @abstractmethod
    def add_columns(self):
        """add the columns"""

    def add_row(self, row):
        """add a row"""
        self.rows.append(row)

    def add_rows(self, n_rows):
        if self.cols:
            self.rows = XRecArray.empty(n_rows, dtype=self.cols.dtype)

    @abstractmethod
    def write_rows(self, writer):
        """amethod to write the rows"""

    def set_data(self, data):
        self.rows = data

    def convert_lines(self, header, lines):
        """Convert lines with header"""
        col_index = self.cols.get_column_index(header)
        n_rows = len(lines)
        recarr = XMaskedRecarray(n_rows,
                                 dtype=self.cols.dtype,
                                 mask=False)
        converters = self.cols.converters
        for c, column in enumerate(self.cols.iteritems()):
            colname, dtype = column
            converter = converters[c]
            idx = col_index[c]
            if idx == -1:
                data = np.empty((n_rows), dtype=dtype)
                mask = np.ones((n_rows), dtype=bool)
            else:
                def convert(value):
                    try:
                        return converter(value), False
                    except ValueError:
                        return converter(0), True
                data_mask = [convert(l[idx]) for l in lines]
                data = [l[0] for l in data_mask]
                mask = [l[1] for l in data_mask]
            table_column = getattr(recarr, colname)
            table_column[:] = data
            table_column.mask[:] = mask

        self.set_data(recarr)


def main():
    pass

if __name__ == '__main__':
    main()