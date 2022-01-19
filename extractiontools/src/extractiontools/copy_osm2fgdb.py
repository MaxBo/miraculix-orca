#!/usr/bin/env python
# coding:utf-8

from typing import List, Tuple
from argparse import ArgumentParser

import logging
logger = logging.getLogger('OrcaLog')

from extractiontools.connection import Connection
from extractiontools.copy2fgdb import Copy2FGDB


class CopyOSM2FGDB(Copy2FGDB):

    def create_views(self):
        """Create the osm views that should be exported"""
        with Connection(login=self.login) as conn:
            self.conn = conn
            self.logger.info('Creating OSM views')
            self.create_dest_schema()
            self.create_railways()
            self.create_amenity()
            self.create_buildings()
            self.create_leisure()
            self.create_landuse()
            self.create_natural()
            self.create_waterways()
            self.create_tourism()
            self.create_shops()
            self.create_boundaries()

            self.conn.commit()

    def create_dest_schema(self):
        """Create the destination schema if not exists"""
        schema = self.schema
        sql = """
CREATE SCHEMA IF NOT EXISTS {schema} AUTHORIZATION group_osm;
        """.format(schema=schema)
        self.run_query(sql)

    def create_geometry_layer(self,
                              columns: str,
                              where_clause: str,
                              view: str,
                              geometrytype: str = 'lines',
                              schema: str = 'osm_layer',
                              sql_comment: str = None,
                              osm_geom_schema: str = 'osm'):
        """Create a linestring layer schema.view, with the given fields
        and the given where-clause"""

        self.logger.info(f'Creating view {schema}.{view}')
        geometrytypes = {'nodes': ('POINT', 'geom'),
                         'lines': ('LINESTRING', 'geom'),
                         'polygons': ('MULTIPOLYGON', 'geom'), }

        try:
            geomtype, geomcolumn = geometrytypes[geometrytype]
        except KeyError:
            raise KeyError('geometrytype {gt} is not valid')

        sql = f"""
        DROP VIEW IF EXISTS {schema}.{view} CASCADE;
        CREATE OR REPLACE VIEW {schema}.{view} AS
        SELECT
          t.id AS id_long,
          t.{geomcolumn}::geometry({geomtype}, {self.target_srid}) AS geom,
          t.tags -> 'name' AS name,
          {columns}

        FROM {osm_geom_schema}.{geometrytype} t
        WHERE {where_clause};
        """
        self.run_query(sql)

        if sql_comment is None:
            sql_comment = f'OSM {geometrytype} where {where_clause}'

        geom_desc = self.get_description(geometrytype, osm_geom_schema) or ''

        sql = f'''
        COMMENT ON VIEW {schema}.{view} IS '{sql_comment}\r\n{geom_desc}';
        '''
        self.run_query(sql, conn=self.conn)

    def create_layer_by_key(self,
                            view: str,
                            keys: List[str],
                            schema: str = 'osm_layer',
                            geometrytype: str = 'lines'):
        """
        Create a linestring layer for all ways having the given keys

        Parameters
        ----------
        view : str
            the layer to create
        keys : str or list of str
            the keys to check for
        schema : str, optional (default= 'osm_layer')
            the schema where the layer should be created
        geometrytype : str(optional, default='lines')
            the geometrytype to create (nodes, lines, polygons)
        """
        columns, where_clause = self.keys2sql(keys)
        # restrict where_clause for nodes to tags with keys to force the use
        # of the partial index
        if geometrytype == 'nodes' and 'tags' in where_clause:
            where_clause += "\nAND tags <> ''::hstore"
        sql_comment = (f"OSM {geometrytype} with "
                       f"{'tags' if len(keys) > 1 else 'tag'} "
                       f"{', '.join(f'<{k}>' for k in keys)}")
        self.create_geometry_layer(columns,
                                   where_clause,
                                   view,
                                   sql_comment=sql_comment,
                                   schema=schema,
                                   geometrytype=geometrytype)

    def keys2sql(self, keys: List[str]) -> Tuple[str, str]:
        """
        convert keys to sql-code

        Parameters
        ----------
        keys : str of list of str
            the keys

        Returns
        -------
        columns : str
        where_clause : str
        """
        if not isinstance(keys, list):
            keys = list(keys)
        where_clause = "t.tags ?| ARRAY{keys}".format(keys=keys)
        columns = ',\n'.join("t.tags -> '{key}' AS {key}".format(key=k)
                             for k in keys)
        return columns, where_clause

    def create_composite_layer(self,
                               view: str,
                               schema: str,
                               *layers: List[str]):
        """Create a composite layer of all geometrytypes"""
        cols = self.conn.get_column_dict(layers[0], schema)
        cols_without_geom = (c for c in cols if c != 'geom')
        self.logger.info(f'Creating composite layer {schema}.{view}')
        if not cols_without_geom:
            raise ValueError("No Columns beside the geom column defined")
        col_str = ', '.join(cols_without_geom)
        sql = """
        SELECT {cols},
        st_pointonsurface(geom)::geometry(POINT, {srid}) AS geom
        FROM {schema}.{layer}"""
        queries = '\nUNION ALL\n'.join(sql.format(cols=col_str,
                                                  schema=schema,
                                                  layer=layer,
                                                  srid=self.target_srid)
                                       for layer in layers)

        sql = """
        DROP VIEW IF EXISTS {schema}.{view} CASCADE;
        CREATE OR REPLACE VIEW {schema}.{view} AS
        {queries};
        """.format(schema=schema, view=view, queries=queries)
        self.run_query(sql)

        description = ''
        for layer in layers:
            desc = self.get_description(layer, schema)
            if desc:
                description += f'{desc}\r\n'
        if description:
            sql = f'''
            COMMENT ON VIEW {schema}.{view} IS '{description}';
            '''
            self.run_query(sql)

    def create_railways(self):
        """Create railways layer"""
        keys = ['railway']
        view = 'railways'
        self.create_layer_by_key(view, keys, geometrytype='lines')

    def create_amenity(self):
        """Create amenities layer"""
        keys = ['amenity']
        view_pnt = 'amenity_pnt'
        self.create_layer_by_key(view_pnt, keys, geometrytype='nodes')
        view_lines = 'amenity_lines'
        self.create_layer_by_key(view_lines, keys, geometrytype='lines')
        view_polys = 'amenity_polys'
        self.create_layer_by_key(view_polys, keys, geometrytype='polygons')
        self.create_composite_layer('amenities',
                                    self.schema,
                                    view_pnt,
                                    view_lines,
                                    view_polys)

    def create_buildings(self):
        """create buildings layer"""
        keys = ['building']
        view = 'buildings'
        self.create_layer_by_key(view, keys, geometrytype='polygons')

    def create_leisure(self):
        """create leisure layer"""
        keys = ['leisure']
        view = 'leisure_pnt'
        self.create_layer_by_key(view, keys, geometrytype='nodes')
        view = 'leisure_lines'
        self.create_layer_by_key(view, keys, geometrytype='lines')
        view = 'leisure_polys'
        self.create_layer_by_key(view, keys, geometrytype='polygons')

    def create_tourism(self):
        """create tourism layer"""
        keys = ['tourism']
        view_pnt = 'tourism_pnt'
        self.create_layer_by_key(view_pnt, keys, geometrytype='nodes')
        view_lines = 'tourism_lines'
        self.create_layer_by_key(view_lines, keys, geometrytype='lines')
        view_polys = 'tourism_polys'
        self.create_layer_by_key(view_polys, keys, geometrytype='polygons')
        self.create_composite_layer('tourism',
                                    self.schema,
                                    view_pnt,
                                    view_lines,
                                    view_polys)

    def create_shops(self):
        """create shops layer"""
        keys = ['shop']
        view_pnt = 'shop_pnt'
        self.create_layer_by_key(view_pnt, keys, geometrytype='nodes')
        view_lines = 'shop_lines'
        self.create_layer_by_key(view_lines, keys, geometrytype='lines')
        view_polys = 'shop_polys'
        self.create_layer_by_key(view_polys, keys, geometrytype='polygons')
        self.create_composite_layer('shops',
                                    self.schema,
                                    view_pnt,
                                    view_lines,
                                    view_polys)

    def create_natural(self):
        """create natural layer"""
        keys = ['natural']
        view = 'natural'
        self.create_layer_by_key(view, keys, geometrytype='polygons')

    def create_landuse(self):
        """create natural layer"""
        keys = ['landuse']
        view = 'osm_landuse'
        self.create_layer_by_key(view, keys, geometrytype='polygons')

    def create_waterways(self):
        """create waterway layer"""
        keys = ['waterway']
        view_lines = 'waterways_lines'
        self.create_layer_by_key(view_lines, keys, geometrytype='lines')
        view_polys = 'waterways_polys'
        self.create_layer_by_key(view_polys, keys, geometrytype='polygons')
        self.create_composite_layer('waterways',
                                    self.schema,
                                    view_lines,
                                    view_polys)

    def create_boundaries(self,
                          osm_geom_schema: str = 'osm'):
        """Create administrative boundaries"""
        sql = f"""
CREATE OR REPLACE VIEW {self.schema}.osm_admin_units(
    id,
    adminlevel,
    name,
    geom,
    admin_type,
    capital,
    heritage,
    population,
    place,
    wikipedia,
    wikidata,
    source)
AS
  SELECT b.id,
         b.adminlevel,
         b.name,
         b.geom,
         r.tags -> 'type'::text AS admin_type,
         r.tags -> 'capital'::text AS capital,
         r.tags -> 'heritage'::text AS heritage,
         r.tags -> 'population'::text AS population,
         r.tags -> 'place'::text AS place,
         r.tags -> 'wikipedia'::text AS wikipedia,
         r.tags -> 'wikidata'::text AS wikidata,
         r.tags -> 'source'::text AS source
  FROM (
         SELECT r_1.id,
                r_1.adminlevel,
                r_1.name,
                st_collect(r_1.geom)::geometry(MultiPolygon, {self.target_srid}) AS geom
         FROM (
                SELECT r_1_1.id,
                       r_1_1.adminlevel,
                       r_1_1.name,
                       st_makepolygon(r_1_1.geom) AS geom
                FROM (
                       SELECT r_2.id,
                              r_2.adminlevel,
                              r_2.name,
                              r_2.member_role,
                              st_geometrytype(r_2.geom) AS st_geometrytype,
                              (st_dump(r_2.geom)).path[1] AS seq,
                              (st_dump(r_2.geom)).geom AS geom
                       FROM (
                              SELECT r_3.id,
                                     r_3.member_role,
                                     r_3.adminlevel,
                                     r_3.name,
                                     st_linemerge(st_collect(r_3.linestring)) AS
                                       geom
                              FROM (
                                     SELECT r_4.id,
                                            r_4.adminlevel,
                                            r_4.name,
                                            w.linestring,
                                            rm.member_role
                                     FROM (
                                            SELECT r_5.id,
                                                   r_5.tags -> 'admin_level':: text AS adminlevel,
                                                   r_5.tags -> 'name'::text AS name
                                            FROM {osm_geom_schema}.relations r_5
                                            WHERE r_5.tags ? 'admin_level'::text
                                          ) r_4,
                                          {osm_geom_schema}.relation_members rm,
                                          {osm_geom_schema}.ways w
                                     WHERE r_4.id = rm.relation_id AND
                                           w.id = rm.member_id AND
                                           rm.member_type = 'W'::bpchar
                                     ORDER BY r_4.id,
                                              rm.sequence_id
                                   ) r_3
                              GROUP BY r_3.id,
                                       r_3.member_role,
                                       r_3.adminlevel,
                                       r_3.name
                            ) r_2
                       WHERE st_isclosed(r_2.geom)
                     ) r_1_1
              ) r_1
         GROUP BY r_1.id,
                  r_1.adminlevel,
                  r_1.name
       ) b,
       {osm_geom_schema}.relations r
  WHERE b.id = r.id;
        """

        self.run_query(sql)


if __name__ == '__main__':

    parser = ArgumentParser(description="Copy Data to File Geodatabase")

    parser.add_argument("-n", '--name', action="store",
                        help="Name of destination database",
                        dest="destination_db", default='extract')

    parser.add_argument("-s", '--srid', action="store",
                        help="srid of the target database", type=int,
                        dest="target_srid")

    parser.add_argument('--host', action="store",
                        help="host",
                        dest="host", default='gis.ggr-planung.de')
    parser.add_argument("-p", '--port', action="store",
                        help="port", type=int,
                        dest="port", default=5432)

    parser.add_argument("-U", '--user', action="store",
                        help="database user",
                        dest="user", default='osm')
    parser.add_argument('--schema', action="store",
                        help="schema",
                        dest="schema", default='osm_layer')
    parser.add_argument('--destschema', action="store",
                        help="destination schema in the FileGDB",
                        dest="dest_schema", default='osm_layer')
    parser.add_argument('--gdbname', action="store",
                        help="Name of the FileGDB to create",
                        dest="gdbname")
    parser.add_argument('--layers', action='store',
                        help='layers to copy,',
                        dest='layers',
                        nargs='+',
                        default=['railways',
                                 'buildings',
                                 'amenity_polys',
                                 ])

    options = parser.parse_args()
    copy2fgdb = CopyOSM2FGDB(options)
    copy2fgdb.get_target_boundary_from_dest_db()
    copy2fgdb.create_views()
    copy2fgdb.copy_layers('FileGDB')
