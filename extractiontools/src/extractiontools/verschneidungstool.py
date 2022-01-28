#!/usr/bin/env python
# coding:utf-8

from argparse import ArgumentParser

from extractiontools.ausschnitt import Extract


class PrepareVerschneidungstool(Extract):
    """
    Prepare Tables and Views for the Verschneidungstool
    """
    schema = 'verschneidungstool'
    tables = {'areas_available': None,
              'column_definitions': None,
              'columns_available': None,
              'current_scenario': None,
              'haltestellen_available': None,
              'projcs2srid': None,
              'projections_available': None,
              'queries': None,
              'resulttables_available': None,
              'scenarios_available': None,
              'schemata_available': None,
              'table_categories': None,
              'tables_to_download': None,
              }

    def final_stuff(self):
        """Final steps in the destiantion db"""
        self.create_views()

    def create_views(self):
        """
        create views for zensus data and
        add a geometry index to views
        """
        sql = f"""
CREATE OR REPLACE VIEW {self.schema}.current_year (
    jahr)
AS
SELECT s.jahr
FROM {self.schema}.scenarios_available s,
    {self.schema}.current_scenario c
WHERE s.scenario = c.scenario;
        """
        self.run_query(sql)
