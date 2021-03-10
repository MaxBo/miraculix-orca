#!/usr/bin/env python
# coding:utf-8

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
            self.create_natural()
            self.create_waterways()
            self.create_tourism()
            self.create_shops()

            self.conn.commit()

    def create_dest_schema(self):
        """Create the destination schema if not exists"""
        schema = self.schema
        sql = """
CREATE SCHEMA IF NOT EXISTS {schema} AUTHORIZATION group_osm;
        """.format(schema=schema)
        self.run_query(sql)

    def create_geometry_layer(self,
                              columns,
                              where_clause,
                              view,
                              geometrytype='lines',
                              schema='osm_layer'):
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

        sql = """
DROP VIEW IF EXISTS {schema}.{view} CASCADE;
CREATE OR REPLACE VIEW {schema}.{view} AS
SELECT
  t.id AS id_long,
  t.{geomcol}::geometry({geomtype}, {srid}) AS geom,
  t.tags -> 'name' AS name,
  {columns}

FROM osm.{geometrytype} t
WHERE {where};
"""
        self.run_query(sql.format(schema=schema,
                                  view=view,
                                  columns=columns,
                                  where=where_clause,
                                  geometrytype=geometrytype,
                                  geomcol=geomcolumn,
                                  geomtype=geomtype,
                                  srid=self.target_srid))

    def create_layer_by_key(self,
                            view,
                            keys,
                            schema='osm_layer',
                            geometrytype='lines'):
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
        self.create_geometry_layer(columns,
                                   where_clause,
                                   view,
                                   schema=schema,
                                   geometrytype=geometrytype)

    def keys2sql(self, keys):
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
                               view,
                               schema,
                               *layers):
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
{queries}
;
        """.format(schema=schema, view=view, queries=queries)
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
