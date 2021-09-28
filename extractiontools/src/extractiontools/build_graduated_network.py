#!/usr/bin/env python
# coding:utf-8

from argparse import ArgumentParser
import ogr

from extractiontools.build_network_car import BuildNetwork


class BuildGraduatedNetwork(BuildNetwork):
    """
    Build graduated Network Analyst-Data in the target Database...
    """

    def __init__(self,
                 detailed_network_area: ogr.Geometry,
                 larger_network_area: ogr.Geometry,
                 **kwargs):
        super().__init__(kwargs)
        self.detailed_network_area = detailed_network_area
        self.larger_network_area = larger_network_area

    def create_streets_view(self):
        """
        Create Views defining the relevant waytypes
        """
        self.logger.info(f'Create graduated Streets View')
        network = self.network
        srid = 4326
        wkb = self.detailed_network_area.ExportToWkb()
        sql = f"""
-- selektiere alle Wege und Straßen, die in waytype2linktype definiert sind
CREATE MATERIALIZED VIEW "{network}".streets AS
SELECT
  w.id,
  min(wtl.linktype_id) linktype_id,
  min(lt.road_category)::char(1) category
FROM
osm.ways w,
classifications.linktypes lt,
classifications.waytype2linktype wtl,
classifications.wt2lt_construction wtc,
(SELECT ST_GeomFromEWKB('SRID={srid};{wkt}') AS geom) da
WHERE (w.tags @> wtl.tags
-- Nimm auch geplante oder im Bau befindliche Straßen mit
OR (w.tags @> wtc.tag1 AND w.tags @> wtc.tag2))
AND wtl.linktype_id=lt.id
AND wtc.linktype_id=lt.id
AND (lt.category <= 'B' OR st_intersects(w.linestring, da.geom)
GROUP BY w.id;
CREATE INDEX streets_idx ON "{network}".streets USING btree(id);
ANALYZE "{network}".streets;
""".format(network=self.network)
        self.run_query(sql)