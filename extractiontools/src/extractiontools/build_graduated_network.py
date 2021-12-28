#!/usr/bin/env python
# coding:utf-8

from argparse import ArgumentParser
from osgeo import ogr

from extractiontools.build_network_car import BuildNetwork


class BuildGraduatedNetwork(BuildNetwork):
    """
    Build graduated Network Analyst-Data in the target Database...
    """

    def __init__(self,
                 detailed_network_area: ogr.Geometry,
                 larger_network_area: ogr.Geometry,
                 **kwargs):
        super().__init__(**kwargs)
        self.detailed_network_area = detailed_network_area
        self.larger_network_area = larger_network_area

    def create_streets_view(self):
        """
        Create Views defining the relevant waytypes
        """
        self.logger.info(f'Create graduated Streets View')
        network = self.network
        srid = 4326
        target_srid = self.srid
        wkt_da = self.detailed_network_area.ExportToWkt()
        wkt_la = self.larger_network_area.ExportToWkt()
        sql = f"""
-- selektiere alle Wege und Straßen, die in waytype2linktype definiert sind
CREATE MATERIALIZED VIEW "{network}".streets AS
SELECT
  w.id,
  min(wtl.linktype_id) linktype_id,
  min(lt.road_category)::char(1) category
FROM
osm.ways w
LEFT JOIN osm.relation_members rm ON (rm.member_id = w.id AND rm.member_type = 'W')
LEFT JOIN osm.relations r ON (rm.relation_id = r.id
AND r.tags ? 'route'
AND r.tags -> 'route' IN ('bus','trolleybus'))
,
classifications.linktypes lt,
classifications.waytype2linktype wtl,
classifications.wt2lt_construction wtc,
(SELECT ST_Transform(ST_GeomFromEWKT('SRID={srid};{wkt_da}'), {target_srid}) AS geom) da,
(SELECT ST_Transform(ST_GeomFromEWKT('SRID={srid};{wkt_la}'), {target_srid}) AS geom) la
WHERE
(w.tags @> wtl.tags
-- Nimm auch geplante oder im Bau befindliche Straßen mit
OR (w.tags @> wtc.tag1 AND w.tags @> wtc.tag2))
AND wtl.linktype_id=lt.id
AND wtc.linktype_id=lt.id
AND (
(( r.id IS NOT NULL OR lt.road_category <= 'B')
AND st_intersects(w.linestring, la.geom))
OR st_intersects(w.linestring, da.geom)
)
GROUP BY w.id;
CREATE INDEX streets_idx ON "{network}".streets USING btree(id);
ANALYZE "{network}".streets;
"""
        self.run_query(sql)
