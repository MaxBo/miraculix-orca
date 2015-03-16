#!/usr/bin/env python
#coding:utf-8
# Author:   --<>
# Purpose:
# Created: 13/03/2015


from extractiontools.transit.gtfs import *


class GTFSVISUM(object):

    def __init__(self, visum, gtfs):
        self.visum = visum
        self.gtfs = gtfs

    @classmethod
    def from_visum(cls, visum, gtfs):
        self = cls(tables)
        n_rows = self.get_n_rows(visum)
        self.add_rows(n_rows)
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


class ShapesFromVisum(Shapes, GTFSfromVISUM):
    """Create """
    @property
    def visum_tables(self):
        return 'LINIENROUTENELEMENT'

    def parse_data(self, visum):
        tables = self.tables
        for s_id in tables.shapes:
            for entry in tables.shapes[s_id]:
                s_seq = entry[0]
                lat, lon  = tables.vertices[entry[1]]

                writer.writerow(( s_id, lat, lon, s_seq ))


if __name__ == '__main__':
    main()