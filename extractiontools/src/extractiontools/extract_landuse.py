#!/usr/bin/env python
# coding:utf-8

from argparse import ArgumentParser
from extractiontools.ausschnitt import Extract


class ExtractLanduse(Extract):
    """
    Extract the landuse data
    """
    tables = {}
    schema = 'landuse'
    role = 'group_osm'
    aster_overviews = [8, 32]
    corine_overviews = [8, 32]

    def __init__(self,
                 source_db,
                 destination_db,
                 gmes,
                 corine,
                 target_srid=31467,
                 **kwargs):
        super().__init__(destination_db=destination_db,
                         target_srid=target_srid,
                         source_db=source_db, **kwargs)
        self.gmes = gmes
        self.corine = corine

    def additional_stuff(self):
        """
        """
        self.wkt = self.get_target_boundary()
        self.extract_oceans()
        self.extract_corine_vector()
        self.extract_aster()
        self.extract_all_corine_raster()
        self.extract_gmes_vector()

    def extract_oceans(self):
        """
        Extract OSM oceans and transform into target srid
        """
        self.logger.info(f'Extracting oceans to {self.schema}.oceans')
        sql = f"""
        SELECT
          c.gid,
          st_transform(c.geom, {self.target_srid})::geometry(MULTIPOLYGON, {self.target_srid}) AS geom
        INTO {self.schema}.oceans
        FROM {self.temp}.oceans c,
        (SELECT ST_GeomFromEWKT('SRID={self.srid};{self.wkt}') AS source_geom) tb
        WHERE
        st_intersects(c.geom, tb.source_geom)
        """
        self.run_query(sql, conn=self.conn)

    def extract_corine_vector(self):
        """
        Extract Corine Landcover data and transform into target srid
        """
        for corine in self.corine:
            self.logger.info(f'Extracting corine landcover data into '
                             f'{self.schema}.{corine}')
            sql = f"""
            SELECT
              c.ogc_fid, c.code, c.id, c.remark,
              st_transform(c.geom, {self.target_srid})::geometry('MULTIPOLYGON',
              {self.target_srid}) AS geom
            INTO {self.schema}.{corine}
            FROM {self.temp}.{corine} c,
            (SELECT ST_GeomFromEWKT('SRID={self.srid};{self.wkt}') AS source_geom) tb
            WHERE
            st_intersects(c.geom, tb.source_geom)
            """
            self.run_query(sql, conn=self.conn)

    def get_corine_raster_name(self, corine):
        corine_raster = '{}_raster'.format(corine)
        return corine_raster

    def extract_all_corine_raster(self):
        """
        Extract the Corine Raster data
        """
        for corine in self.corine:
            corine_raster = self.get_corine_raster_name(corine)
            self.extract_corine_raster(corine_raster)

    def extract_corine_raster(self, corine_raster_table):
        """
        Extract Corine Raster data and transform into target srid

        """
        # Corine Raster data is given in LAEA-ETRS (EPSG:3035)
        corine_raster_srid = 3035
        tables = [corine_raster_table] + \
            ['o_{ov}_{rt}'.format(ov=ov,
                                  rt=corine_raster_table)
             for ov in self.corine_overviews]

        for tn in tables:
            self.logger.info(f'Extracting corine raster data '
                             f'into {self.schema}.{tn}')
            sql = f"""
            CREATE TABLE {self.schema}.{tn}
            (rid serial PRIMARY KEY,
              rast raster,
              filename text);

            INSERT INTO {self.schema}.{tn} (rast, filename)
            SELECT
              rast,
              r.filename
            FROM {self.temp}.{tn} r,
            (SELECT ST_GeomFromEWKT('SRID={self.srid};{self.wkt}') AS source_geom) tb
            WHERE
            st_intersects(r.rast, st_transform(tb.source_geom, {corine_raster_srid}));
            """
            self.run_query(sql, conn=self.conn)

    def extract_aster(self):
        """
        Extract Aster Digital Elevation data and transform into target srid

        """
        self.raster_table = 'aster'
        tables = [self.raster_table] + \
            ['o_{ov}_{rt}'.format(ov=ov,
                                  rt=self.raster_table)
             for ov in self.aster_overviews]

        for tn in tables:
            self.logger.info(f'Extracting Aster elevation data into '
                             f'{self.schema}.{tn}')
            sql = f"""
            CREATE TABLE {self.schema}.{tn}
            (rid serial PRIMARY KEY,
              rast raster,
              filename text);

            INSERT INTO {self.schema}.{tn} (rast, filename)
            SELECT
              rast,
              r.filename
            FROM {self.temp}.{tn} r,
            (SELECT ST_GeomFromEWKT('SRID={self.srid};{self.wkt}') AS source_geom) tb
            WHERE
            st_intersects(r.rast, tb.source_geom);
            """
            self.run_query(sql, conn=self.conn)

        self.logger.info(f'Creating view {self.schema}.aster_centroids')
        # raster points
        sql = """
        CREATE MATERIALIZED VIEW {schema}.aster_centroids AS
         SELECT DISTINCT st_transform((b.a).geom, {target_srid})::geometry(POINT, {target_srid}) AS geom,
            (b.a).val AS val
           FROM ( SELECT st_pixelascentroids(aster.rast) AS a
                   FROM {temp}.aster AS aster) b
        WITH NO DATA;
        """
        self.run_query(sql.format(temp=self.temp,
                                  schema=self.schema,
                                  target_srid=self.target_srid),
                       conn=self.conn)

    def extract_gmes_vector(self):
        """
        Extract GMES Urban Atlas Landcover data and transform into target srid
        """
        for gmes in self.gmes:
            self.logger.info(f'Extracting GMES Urban Atlas boundaries into '
                             f'{self.schema}.{gmes}_boundary')
            columns = self.conn.get_column_dict(
                f'"{self.temp}"."{gmes}_boundary"')
            columns.pop('geom')
            cols = ', '.join([f'c."{col}"' for col in columns])
            sql = f"""
            SELECT
              {cols},
              st_multi(st_transform(c.geom, {self.target_srid}))::geometry('MULTIPOLYGON',
              {self.target_srid}) AS geom
            INTO "{self.schema}"."{gmes}_boundary"
            FROM "{self.temp}"."{gmes}_boundary" c,
            (SELECT ST_GeomFromEWKT('SRID={self.srid};{self.wkt}') AS source_geom) tb
            WHERE
            st_intersects(c.geom, tb.source_geom)
                """
            self.run_query(sql, conn=self.conn)

            self.logger.info(f'Extracting GMES Urban Atlas landcover data into '
                             f'"{self.schema}"."{gmes}"')
            columns = self.conn.get_column_dict(
                f'"{self.temp}"."{gmes}"')
            columns.pop('geom')
            cols = ', '.join([f'c."{col}"' for col in columns])
            sql = f"""
            SELECT
              {cols},
              st_multi(st_transform(c.geom, {self.target_srid}))::geometry('MULTIPOLYGON',
              {self.target_srid}) AS geom
            INTO "{self.schema}"."{gmes}"
            FROM "{self.temp}"."{gmes}" c,
            (SELECT ST_GeomFromEWKT('SRID={self.srid};{self.wkt}') AS source_geom) tb
            WHERE
            st_intersects(c.geom, tb.source_geom)
            """
            self.run_query(sql, conn=self.conn)

            sql = f"""
            SELECT EXISTS (SELECT *
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = '{self.temp}'
            AND TABLE_NAME = '{gmes}_urban_core')
            """
            cur = self.conn.cursor()
            cur.execute(sql)
            urban_core_exists = cur.fetchone()[0]

            if urban_core_exists:
                self.logger.info(f'Extracting GMES Urban Atlas urban core into '
                                 f'{self.schema}.{gmes}_urban_core')
                columns = self.conn.get_column_dict(
                    f'"{self.temp}"."{gmes}_urban_core"')
                columns.pop('geom')
                cols = ', '.join([f'c."{col}"' for col in columns])
                sql = f"""
                SELECT
                  {cols},
                  st_multi(st_transform(c.geom, {self.target_srid}))::geometry('MULTIPOLYGON',
                  {self.target_srid}) AS geom
                INTO "{self.schema}"."{gmes}_urban_core"
                FROM "{self.temp}"."{gmes}_urban_core" c,
                (SELECT ST_GeomFromEWKT('SRID={self.srid};{self.wkt}') AS source_geom) tb
                WHERE
                st_intersects(c.geom, tb.source_geom)
                """
                self.run_query(sql, conn=self.conn)

    def create_index(self):
        """
        CREATE INDEX
        """
        self.create_index_oceans()
        self.create_index_corine()
        self.create_index_gmes()
        self.add_raster_index_and_overviews(self.aster_overviews,
                                            self.schema,
                                            self.raster_table)
        self.create_corine_raster_index()

    def create_index_corine(self):
        """ Corine landcover Index"""

        self.logger.info(f'Creating indexes for corine landcover data')
        sql = """
        ALTER TABLE {schema}.{corine} ADD PRIMARY KEY (ogc_fid);
        CREATE INDEX {corine}_geom_idx
          ON {schema}.{corine}
          USING gist(geom);
        CREATE INDEX idx_{corine}_code
          ON {schema}.{corine}
          USING btree(code);
        ALTER TABLE {schema}.{corine} CLUSTER ON {corine}_geom_idx;
        """
        for corine in self.corine:

            self.run_query(sql.format(schema=self.schema,
                                      corine=corine), conn=self.conn)
            self.tables2cluster.append('{schema}.{corine}'.format(
                schema=self.schema,
                corine=corine))

    def create_index_gmes(self):
        """ Corine GMES Index"""
        self.logger.info(f'Creating indexes for GMES landcover data')
        sql_boundary = """
        ALTER TABLE "{schema}"."{gmes}_boundary" ADD PRIMARY KEY (ogc_fid);
        CREATE INDEX {gmes}_boundary_geom_idx
        ON "{schema}"."{gmes}_boundary"
        USING gist(geom);
        """

        sql_urban_core = """
        ALTER TABLE "{schema}"."{gmes}_urban_core" ADD PRIMARY KEY (ogc_fid);
        CREATE INDEX {gmes}_urban_core_geom_idx
        ON "{schema}"."{gmes}_urban_core"
        USING gist(geom);
        """

        sql_ua = """
        ALTER TABLE "{schema}"."{gmes}" ADD PRIMARY KEY (ogc_fid);
        CREATE INDEX {gmes}_geom_idx
          ON "{schema}"."{gmes}"
          USING gist(geom);
        CREATE INDEX idx_{gmes}_code
          ON "{schema}"."{gmes}"
          USING btree({code});
        ALTER TABLE {schema}.{gmes} CLUSTER ON {gmes}_geom_idx;
        """
        for gmes in self.gmes:
            cols = self.conn.get_column_dict(f'"{self.schema}"."{gmes}"')
            code = [col for col in cols if col.startswith('code')][0]
            self.logger.info(sql_ua.format(
                schema=self.schema, gmes=gmes, code=code))
            self.run_query(sql_ua.format(
                schema=self.schema, gmes=gmes, code=code),
                conn=self.conn)
            self.logger.info(sql_boundary.format(
                schema=self.schema, gmes=gmes))

            self.run_query(sql_boundary.format(
                schema=self.schema, gmes=gmes),
                conn=self.conn)
            sql = f"""
            SELECT EXISTS (SELECT *
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = '{self.schema}'
            AND TABLE_NAME = '{gmes}_urban_core')
            """
            cur = self.conn.cursor()
            cur.execute(sql)
            urban_core_exists = cur.fetchone()[0]
            if urban_core_exists:
                self.run_query(sql_urban_core.format(
                    schema=self.schema, gmes=gmes), conn=self.conn)

            self.tables2cluster.append(f'"{self.schema}"."{gmes}"')

    def create_index_oceans(self):
        """Oceans Index"""
        self.logger.info(f'Creating indexes for oceans')
        sql = """
        -- oceans
        ALTER TABLE {schema}.oceans ADD PRIMARY KEY (gid);
        CREATE INDEX oceans_geom_idx
        ON {schema}.oceans
        USING gist(geom);
        """
        self.run_query(sql.format(schema=self.schema), conn=self.conn)

    def create_corine_raster_index(self):
        """
        add index to all corine rasters requiered in the arguments
        """
        self.logger.info(f'Creating indexes for corine raster data')
        for corine in self.corine:
            corine_raster = self.get_corine_raster_name(corine)
            self.add_raster_index_and_overviews(overviews=self.corine_overviews,
                                                schema=self.schema,
                                                tablename=corine_raster)


if __name__ == '__main__':

    parser = ArgumentParser(description="Extract Data for Model")

    parser.add_argument("-n", '--name', action="store",
                        help="Name of destination database",
                        dest="destination_db", default='extract')

    parser.add_argument('--host', action="store",
                        help="host",
                        dest="host", default='localhost')
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

    parser.add_argument('--gmes', action="store", nargs='*',
                        help="specify the corine datasets",
                        dest="gmes", default=['ua2012'])

    options = parser.parse_args()

    extract = ExtractLanduse(source_db=options.source_db,
                             destination_db=options.destination_db,
                             gmes=options.gmes,
                             corine=options.corine)
    extract.set_login(host=options.host,
                      port=options.port,
                      user=options.user)
    extract.get_target_boundary_from_dest_db()
    extract.extract()
