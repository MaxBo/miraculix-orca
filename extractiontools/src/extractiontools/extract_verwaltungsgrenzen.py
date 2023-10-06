#!/usr/bin/env python
# coding:utf-8

from extractiontools.ausschnitt import Extract


class ExtractVerwaltungsgrenzen(Extract):
    """
    Extract the osm data
    """
    schema = 'verwaltungsgrenzen'
    role = 'group_osm'

    def final_stuff(self):
        self.copy_constraints_and_indices(
            self.schema, [t for t in self.tables if t in self.new_tables])


class ExtractFirmsNeighbourhoods(ExtractVerwaltungsgrenzen):
    """
    Extract the firms from bedirect and the IRS Neighbourhoods
    """
    schema = 'firms'
