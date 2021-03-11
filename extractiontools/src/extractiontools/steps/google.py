import orca
import math
from orcadjango.decorators import meta
from extractiontools.ausschnitt import Extract
from extractiontools.connection import Connection
from extractiontools.utils.google_api import GooglePlacesAPI


@meta(group='(7) Google', required='create_db')
@orca.step()
def google_places(database: str, google_key: str, places_table: str,
                  places_type: str, places_keyword: str,
                  project_area: 'ogr.Geometry', places_search_radius:  int):
    google = GoogleApp(database, boundary=project_area)
    google.get_places(google_key, keyword=places_keyword, typ=places_type,
                      table=places_table, search_radius=places_search_radius)


class GoogleApp(Extract):
    schema = 'google'

    def get_places(self, key, keyword=None, typ=None, table='places'):
        search_radius = 50000
        #point_distance = math.floor(search_radius * math.sqrt(2))

        with Connection(login=self.login) as conn:

            sql = f'''
            SELECT
            st_x(b.point) x, st_y(b.point) y
            FROM (
              SELECT st_transform(st_centroid(a.geom),4326) point
              FROM (
                SELECT (
                  ST_HexagonGrid({search_radius},
                  ST_Transform(geom, {self.target_srid}))).*
                FROM meta.boundary
                WHERE name=%(boundary_name)s
              ) a
            ) b
            '''
            self.logger.debug(sql)
            cursor = conn.cursor()
            cursor.execute(sql, {'boundary_name': self.boundary_name})
            points = cursor.fetchall()

            for point in points:
                api = GooglePlacesAPI(key)
                res = api.query_places_nearby(point.y, point.x,
                                              radius=search_radius,
                                              keyword=keyword, typ=typ)

