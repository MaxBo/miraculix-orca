#!/usr/bin/env python
#coding:utf-8
# Author:   --<>
# Purpose:
# Created: 13/03/2015

from abc import ABCMeta, abstractproperty, abstractmethod

class HeaderException(Exception):
    def __init__(self, table):
        self.table = table


class Vertices(object):
    def __init__(self):
        pass


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

