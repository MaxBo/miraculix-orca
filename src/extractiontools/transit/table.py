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
import logging
logger = logging.getLogger()
formatter = logging.Formatter('%(asctime)s %(message)s', datefmt='%H:%M:%S')
sh = logging.StreamHandler()
sh.setFormatter(formatter)
logger.addHandler(sh)
logger.level = logging.DEBUG

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
        vals = self.values()
        #formats = [getattr(v, 'dtype', v)
                   #if isinstance(getattr(v, 'dtype', v), np.dtype)
                   #else v
                   #for v in vals]
        dtype = {'names': self.keys(),
                'formats': vals,}
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
        return self.rows.shape[0]

    def __getattr__(self, attr):
        if attr in self.cols:
            return getattr(self.rows, attr)
        return super(Table, self).__getattribute__(attr)

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

    def add_pkey(self, *args):
        """Add primary key columns

        Parameters
        ----------
        colname [, colname [, colname...]]: str
            the columns that form the primary key
        """
        assert [isinstance(p, (str, unicode)) for p in args]
        self.pkey_cols = list(args)

    def get_columns_by_names(self, colnames):
        """
        return columns defined by names
        if colnames are several names, then return a list of
        tuples, which are hashable
        otherwise, return a XMaskedArray

        Parameters
        ----------
        colnames : str or list/tuple of str
            the colnames to return
        """
        if len(colnames) == 1:
            return getattr(self.rows, colnames[0])
        elif len(colnames) == 0:
            raise ValueError('No key defined')

        new_arr = np.rec.fromarrays((getattr(self.rows, col)
                                  for col in colnames))
        return new_arr

    def get_columns_by_names_hashable(self, colnames):
        """
        return columns defined by names
        if colnames are several names, then return a list of
        tuples, which are hashable
        otherwise, return a XMaskedArray

        Parameters
        ----------
        colnames : str or list/tuple of str
            the colnames to return
        """
        arr = self.get_columns_by_names(colnames)
        if isinstance(arr, np.core.records.recarray):
            return arr.view(type='S%s' %arr.itemsize)
        return arr

    @property
    def pkey(self):
        return self.get_columns_by_names(self.pkey_cols)

    @property
    def pkey_hashed(self):
        return self.get_columns_by_names_hashable(self.pkey_cols)

    def add_rows(self, n_rows):
        if self.cols:
            defaults = [self.defaults[colname] if self.defaults[colname] is not None
                        else self.cols.converters[c](0)
                        for c, colname
                        in enumerate(self.cols)]
            self.rows = XRecArray.empty(n_rows, dtype=self.cols.dtype)
            self.rows.fill(tuple(defaults))

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

    def get_rows_by_key(self, col_key, colname_value, data,
                         missing_value=-1):
        col_values = getattr(self, colname_value)
        d = dict(zip(col_key, col_values))
        def get_values(key):
            name = d.get(key, missing_value)
            return name
        mp = np.vectorize(get_values, otypes=col_values.dtype.char)
        val = mp(data.view(np.ndarray))
        return np.ma.masked_equal(val, missing_value)

    def get_rows_by_pkey(self, colname_value, data, missing_value=-1):
        col_key = self.pkey_hashed
        return self.get_rows_by_key(col_key, colname_value,
                                    data, missing_value)


def main():
    pass

if __name__ == '__main__':
    main()