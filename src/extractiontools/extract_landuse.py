#!/usr/bin/env python
#coding:utf-8

from argparse import ArgumentParser

from extractiontools.ausschnitt import Extract, BBox, logger


class ExtractLanduse(Extract):
    """
    Extract the landuse data
    """
    tables = {}
    schema = 'landuse'
    role = 'group_osm'

    def additional_stuff(self):
        """
        """
        self.extract_oceans()
        self.extract_corine()
        self.extract_aster()
        self.extract_corine_raster()

    def extract_oceans(self):
        """
        Extract OSM oceans and transform into target srid
        """
        sql = """
SELECT
  c.gid,
  st_transform(c.geom, {target_srid})::geometry(MULTIPOLYGON, {target_srid}) AS geom
INTO {temp}.oceans
FROM {schema}.oceans c, {temp}.boundary tb
WHERE
c.geom && tb.source_geom
        """
        self.run_query(sql.format(temp=self.temp, schema=self.schema,
                                  target_srid=self.target_srid),
                       conn=self.conn0)

    def extract_corine(self):
        """
        Extract Corine Landcover data and transform into target srid
        """
        sql = """
    SELECT
      c.fid, c.code, c.id, c.remark,
      st_transform(c.geom, {target_srid})::geometry('MULTIPOLYGON',
      {target_srid}) AS geom
    INTO {temp}.{corine}
    FROM {schema}.{corine} c, {temp}.boundary tb
    WHERE
    c.geom && tb.source_geom
            """
        for corine in self.options.corine:
            corine_raster = self.get_corine_raster_name(corine)

            self.run_query(sql.format(temp=self.temp, schema=self.schema,
                                      target_srid=self.target_srid,
                                      corine=corine_raster),
                           conn=self.conn0)

    def get_corine_raster_name(self, corine):
        corine_raster = '{}_raster'.format(corine)
        return corine_raster

    def extract_corine_raster(self):
        """
        Extract the Corine Raster data
        """
        for corine in self.options.corine:
            self.extract_corine(corine)

    def extract_one_corine_raster(self, corine_raster_table):
        """
        Extract Corine Raster data and transform into target srid

        """
        # Corine Raster data is given in LAEA-ETRS (EPSG:3035)
        corine_raster_srid = 3035
        #self.corine_overviews = [8, 32]
        self.corine_overviews = []
        tables = [corine_raster_table] + \
            ['o_{ov}_{rt}'.format(ov=ov,
                                  rt=corine_raster_table)
             for ov in self.corine_overviews]

        sql = """
CREATE TABLE {temp}.{tn}
(rid serial PRIMARY KEY,
  rast raster,
  filename text);

INSERT INTO {temp}.{tn} (rast, filename)
SELECT
  rast,
  r.filename
FROM {schema}.{tn} r, {temp}.boundary tb
WHERE
r.rast && st_transform(tb.source_geom, {corine_srid});
        """
        for tn in tables:
            self.run_query(sql.format(temp=self.temp,
                                      schema=self.schema,
                                      target_srid=self.target_srid,
                                      tn=tn,
                                      corine_srid=corine_raster_srid),
                           conn=self.conn0)


    def extract_aster(self):
        """
        Extract Aster Digital Elevation data and transform into target srid

        """
        self.raster_table = 'aster'
        self.overviews = [8, 32]
        tables = [self.raster_table] + \
            ['o_{ov}_{rt}'.format(ov=ov,
                                  rt=self.raster_table)
             for ov in self.overviews]

        sql = """
CREATE TABLE {temp}.{tn}
(rid serial PRIMARY KEY,
  rast raster,
  filename text);

INSERT INTO {temp}.{tn} (rast, filename)
SELECT
  rast,
  r.filename
FROM {schema}.{tn} r, {temp}.boundary tb
WHERE
r.rast && tb.source_geom;
        """
        for tn in tables:
            self.run_query(sql.format(temp=self.temp, schema=self.schema,
                                      target_srid=self.target_srid,
                                      tn=tn),
                           conn=self.conn0)

        # raster points
        sql = """
CREATE MATERIALIZED VIEW {schema}.aster_centroids AS
 SELECT DISTINCT st_transform((b.a).geom, {target_srid})::geometry(POINT, {target_srid}) AS geom,
    (b.a).val AS val
   FROM ( SELECT st_pixelascentroids(aster.rast) AS a
           FROM {schema}.aster AS aster) b
WITH NO DATA;
        """
        self.run_query(sql.format(schema=self.temp,
                                  target_srid=self.target_srid),
                       conn=self.conn0)

    def create_index(self):
        """
        CREATE INDEX
        """
        self.create_index_oceans()
        self.create_index_corine()
        self.add_aster_index()
        self.calc_aster_centroids()
        self.create_corine_raster_index()

    def create_index_corine(self):
        """ Corine landcover Index"""

        sql = """
    ALTER TABLE {schema}.{corine} ADD PRIMARY KEY (fid);
    CREATE INDEX {corine}_geom_idx
      ON {schema}.{corine}
      USING gist(geom);
    CREATE INDEX idx_{corine}_code
      ON {schema}.{corine}
      USING btree(code);
    ALTER TABLE {schema}.{corine} CLUSTER ON {corine}_geom_idx;
    """
        for corine in self.options.corine:

            self.run_query(sql.format(schema=self.schema,
                                      corine=corine), conn=self.conn1)
            self.tables2cluster.append('{schema}.{corine}'.format(
                schema=self.schema,
                corine=corine))

    def create_index_oceans(self):
        """Oceans Index"""
        sql = """
    -- oceans
    ALTER TABLE {schema}.oceans ADD PRIMARY KEY (gid);
    CREATE INDEX oceans_geom_idx
    ON {schema}.oceans
    USING gist(geom);
    """
        self.run_query(sql.format(schema=self.schema), conn=self.conn1)

    def calc_aster_centroids(self):
        """
        refresh the materialized view with the aster centroids
        """
        sql = """
REFRESH MATERIALIZED VIEW {schema}.aster_centroids;
CREATE INDEX idx_aster_centroids_geom ON {schema}.aster_centroids USING gist(geom);
ANALYZE {schema}.aster_centroids;
        """
        self.run_query(sql.format(schema=self.schema), conn=self.conn1)

    def add_aster_index(self):
        sql = """
CREATE INDEX idx_{tn}_geom ON {schema}.{tn} USING gist(st_convexhull(rast));
SELECT AddRasterConstraints('{schema}', '{tn}', 'rast', TRUE, TRUE, TRUE, TRUE,
                            TRUE, TRUE, TRUE, TRUE, TRUE, TRUE, TRUE, TRUE);
        """
        self.run_query(sql.format(schema=self.schema, tn=self.raster_table),
                       conn=self.conn1)
        self.conn1.commit()
        self.add_overview_index(self.overviews, self.raster_table)

    def add_overview_index(self, overviews, raster_table):
        """
        Add an index to all given overview rasters for the given raster table
        """

        sql = """
CREATE INDEX idx_o_{ov}_{tn}_geom ON {schema}.o_{ov}_{tn}
USING gist(st_convexhull(rast));
SELECT AddOverviewConstraints('{schema}', 'o_{ov}_{tn}', 'rast',
                              '{schema}', '{tn}', 'rast', {ov});
SELECT AddRasterConstraints('{schema}', 'o_{ov}_{tn}', 'rast', TRUE, TRUE, TRUE, TRUE,
                            TRUE, TRUE, TRUE, TRUE, TRUE, TRUE, TRUE, TRUE);
        """
        for ov in overviews:
            self.run_query(sql.format(schema=self.schema,
                                      tn=raster_table,
                                      ov=ov), conn=self.conn1)

    def add_one_corine_raster_index(self, corine_raster):
        sql = """
CREATE INDEX idx_{tn}_geom ON {schema}.{tn} USING gist(st_convexhull(rast));
SELECT AddRasterConstraints('{schema}', '{tn}', 'rast', TRUE, TRUE, TRUE, TRUE,
                            TRUE, TRUE, TRUE, TRUE, TRUE, TRUE, TRUE, TRUE);
        """
        self.run_query(sql.format(schema=self.schema, tn=corine_raster),
                       conn=self.conn1)
        self.conn1.commit()
        self.add_overview_index(self.corine_overviews, corine_raster)

    def create_corine_raster_index(self):
        """
        add index to all corine rasters requiered in the arguments
        """
        for corine in self.options.corine:
            corine_raster = self.get_corine_raster_name(corine)
            self.add_one_corine_raster_index(corine_raster)


if __name__ == '__main__':

    parser = ArgumentParser(description="Extract Data for Model")

    parser.add_argument("-n", '--name', action="store",
                        help="Name of destination database",
                        dest="destination_db", default='extract')

    parser.add_argument('--host', action="store",
                        help="host",
                        dest="host", default='gis.ggr-planung.de')
    parser.add_argument("-p", '--port', action="store",
                        help="port", type=int,
                        dest="port", default=5432)

    parser.add_argument("-U", '--user', action="store",
                        help="database user",
                        dest="user", default='osm')
    parser.add_argument('--source-db', action="store",
                        help="source database",
                        dest="source_db", default='europe')

    parser.add_argument('--corine', action="store", nargs='*',
                        help="specify the corine datasets",
                        dest="corine", default=['clc12'])


    options = parser.parse_args()

    extract = ExtractLanduse(source_db=options.source_db,
                             destination_db=options.destination_db,
                             recreate_db=False)
    extract.set_login(host=options.host,
                      port=options.port,
                      user=options.user)
    extract.get_target_boundary_from_dest_db()
    extract.extract()
