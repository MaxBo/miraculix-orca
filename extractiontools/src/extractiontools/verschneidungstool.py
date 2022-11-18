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
        self.create_indices()

    def create_views(self):
        """
        create views for current year
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

    def create_indices(self):
        """
        Add indeces
        """
        sql = f"""
ALTER TABLE {self.schema}.current_scenario
ADD PRIMARY KEY(scenario);
ALTER TABLE {self.schema}.scenarios_available
ADD PRIMARY KEY(scenario);

ALTER TABLE {self.schema}.schemata_available
ADD PRIMARY KEY(name);

CREATE SEQUENCE {self.schema}.areas_available_id_seq
MAXVALUE 2147483647;
ALTER TABLE {self.schema}.areas_available
ADD PRIMARY KEY(id),
ALTER COLUMN id SET DEFAULT nextval('{self.schema}.areas_available_id_seq'::text);

CREATE SEQUENCE {self.schema}.haltestellen_available_id_seq
MAXVALUE 2147483647;
ALTER TABLE {self.schema}.haltestellen_available
ADD PRIMARY KEY(name),
ALTER COLUMN id SET DEFAULT nextval('{self.schema}.haltestellen_available_id_seq'::text),
ALTER COLUMN id SET NOT NULL;
CREATE UNIQUE INDEX haltestellen_available_id_key ON {self.schema}.haltestellen_available
  USING btree (id);

ALTER TABLE {self.schema}.projcs2srid
ADD PRIMARY KEY(name);
ALTER TABLE {self.schema}.projections_available
ADD PRIMARY KEY(srid),
ADD CONSTRAINT projections_available_srid_fkey FOREIGN KEY (srid)
    REFERENCES public.spatial_ref_sys(srid)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION
    NOT DEFERRABLE
;

CREATE SEQUENCE {self.schema}.queries_id_seq
MAXVALUE 2147483647;
ALTER TABLE {self.schema}.queries
ADD PRIMARY KEY(id),
ALTER COLUMN id SET DEFAULT nextval('{self.schema}.queries_id_seq'::text);

ALTER TABLE {self.schema}.resulttables_available
ADD PRIMARY KEY(schema_table);

CREATE SEQUENCE {self.schema}.table_categories_id_seq
MAXVALUE 2147483647;
ALTER TABLE {self.schema}.table_categories
ADD PRIMARY KEY(id),
ALTER COLUMN id SET DEFAULT nextval('{self.schema}.table_categories_id_seq'::text);
CREATE UNIQUE INDEX table_categories_name_key ON {self.schema}.table_categories
  USING btree (name);

CREATE SEQUENCE {self.schema}.tables_to_download_id_seq
MAXVALUE 2147483647;
ALTER TABLE {self.schema}.tables_to_download
ADD PRIMARY KEY(name),
ALTER COLUMN id SET DEFAULT nextval('{self.schema}.tables_to_download_id_seq'::text),
ALTER COLUMN id SET NOT NULL;

CREATE SEQUENCE {self.schema}.columns_available_id_seq
ALTER TABLE {self.schema}.columns_available
ADD PRIMARY KEY(id),
ALTER COLUMN id SET DEFAULT nextval('{self.schema}.columns_available_id_seq'::text);
ADD CONSTRAINT columns_available_fk FOREIGN KEY (table_type)
    REFERENCES {self.schema}.table_categories(name)
    ON DELETE NO ACTION
    ON UPDATE CASCADE
    NOT DEFERRABLE,
ADD CONSTRAINT columns_available_fk1 FOREIGN KEY (resulttable)
    REFERENCES {self.schema}.resulttables_available(schema_table)
    ON DELETE SET NULL
    ON UPDATE CASCADE
    NOT DEFERRABLE
;
ALTER TABLE {self.schema}.column_definitions
ADD PRIMARY KEY(table_category),
ADD CONSTRAINT column_definitions_fk FOREIGN KEY (table_category)
    REFERENCES {self.schema}.table_categories(name)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION
    NOT DEFERRABLE
;
CREATE UNIQUE INDEX columns_available_column_idx ON {self.schema}.columns_available
  USING btree ("column", resulttable );
        """
        self.run_query(sql)
