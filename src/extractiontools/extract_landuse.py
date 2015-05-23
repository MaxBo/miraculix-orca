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
        self.extract_corine()
        self.extract_aster()

    def extract_corine(self):
        """
        Extract Corine Landcover data and transform into target srid
        """
        sql = """
SELECT
  c.fid, c.code_06, c.id, c.remark,
  st_transform(c.geom, {target_srid})::geometry('MULTIPOLYGON', {target_srid}) AS geom
INTO {temp}.clc06
FROM {schema}.clc06 c, meta.boundary tb
WHERE
c.geom && tb.source_geom
        """
        self.run_query(sql.format(temp=self.temp, schema=self.schema,
                                  target_srid=self.target_srid),
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
FROM {schema}.{tn} r, meta.boundary tb
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
 SELECT DISTINCT st_transform((b.a).geom, {target_srid}) AS geom,
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
        sql = """
ALTER TABLE {schema}.clc06 ADD PRIMARY KEY (fid);
CREATE INDEX clc06_geom_idx
  ON {schema}.clc06
  USING gist(geom);
CREATE INDEX idx_clc06_code
  ON {schema}.clc06
  USING btree(code_06);
ALTER TABLE {schema}.clc06 CLUSTER ON clc06_geom_idx;
"""
        self.run_query(sql.format(schema=self.schema), conn=self.conn1)
        self.tables2cluster.append('{schema}.clc06'.format(schema=self.schema))

        sql = """
CREATE INDEX idx_{tn}_geom ON {schema}.{tn} USING gist(st_convexhull(rast));
SELECT AddRasterConstraints('{schema}', '{tn}', 'rast', TRUE, TRUE, TRUE, TRUE,
                            TRUE, TRUE, TRUE, TRUE, TRUE, TRUE, TRUE, TRUE);
        """
        self.run_query(sql.format(schema=self.schema, tn=self.raster_table),
                       conn=self.conn1)
        self.conn1.commit()
        sql = """
CREATE INDEX idx_o_{ov}_{tn}_geom ON {schema}.o_{ov}_{tn}
USING gist(st_convexhull(rast));
SELECT AddOverviewConstraints('{schema}', 'o_{ov}_{tn}', 'rast',
                              '{schema}', '{tn}', 'rast', {ov});
SELECT AddRasterConstraints('{schema}', 'o_{ov}_{tn}', 'rast', TRUE, TRUE, TRUE, TRUE,
                            TRUE, TRUE, TRUE, TRUE, TRUE, TRUE, TRUE, TRUE);
        """
        for ov in self.overviews:
            self.run_query(sql.format(schema=self.schema,
                                      tn=self.raster_table,
                                      ov=ov), conn=self.conn1)

        # Centroid
        sql = """
REFRESH MATERIALIZED VIEW {schema}.aster_centroids;
CREATE INDEX idx_aster_centroids_geom ON {schema}.aster_centroids USING gist(geom);
ANALYZE {schema}.aster_centroids;
        """
        self.run_query(sql.format(schema=self.schema), conn=self.conn1)



if __name__ == '__main__':

    parser = ArgumentParser(description="Extract Data for Model")

    parser.add_argument("-n", '--name', action="store",
                        help="Name of destination database",
                        dest="destination_db", default='extract')

    parser.add_argument("-s", '--srid', action="store",
                        help="srid of the target database", type=int,
                        dest="srid", default='31467')

    parser.add_argument("-t", '--top', action="store",
                        help="top", type=float,
                        dest="top", default=54.65)
    parser.add_argument("-b", '--bottom,', action="store",
                        help="bottom", type=float,
                        dest="bottom", default=54.6)
    parser.add_argument("-r", '--right', action="store",
                        help="right", type=float,
                        dest="right", default=10.0)
    parser.add_argument("-l", '--left', action="store",
                        help="left", type=float,
                        dest="left", default=9.95)

    parser.add_argument('--host', action="store",
                        help="host",
                        dest="host", default='gis.ggr-planung.de')
    parser.add_argument("-p", '--port', action="store",
                        help="port", type=int,
                        dest="port", default=5432)

    parser.add_argument("-U", '--user', action="store",
                        help="database user",
                        dest="user", default='max')
    parser.add_argument('--source-db', action="store",
                        help="source database",
                        dest="source_db", default='dplus')

    options = parser.parse_args()

    bbox = BBox(top=options.top, bottom=options.bottom,
                left=options.left, right=options.right)
    extract = ExtractLanduse(source_db=options.source_db,
                             destination_db=options.destination_db,
                             target_srid=options.srid,
                             recreate_db=False)
    extract.set_login(host=options.host, port=options.port, user=options.user)
    extract.get_target_boundary(bbox)
    extract.extract()
