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

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.set_target_boundary(self.boundary)
        self.update_boundaries()

    def get_places(self, key, search_radius=5000, keyword=None, typ=None,
                   table='places'):
        #point_distance = math.floor(search_radius * math.sqrt(2))

        with Connection(login=self.login) as conn:

            sql = f'''
            SELECT
            st_x(a.point) x, st_y(a.point) y
            FROM (
              SELECT st_transform(st_centroid(hex.geom),4326) point
              FROM
                  meta.boundary m
                  CROSS JOIN
                  ST_HexagonGrid({search_radius}, m.geom) AS hex
              WHERE
                  name=%(boundary_name)s
                  AND
                  ST_Intersects(m.geom, hex.geom)
            ) a
            '''
            self.logger.debug(sql)
            cursor = conn.cursor()
            cursor.execute(sql, {'boundary_name': self.boundary_name})
            points = cursor.fetchall()

            for point in points:
                api = GooglePlacesAPI(key)
                res = api.query_places_nearby(
                    point.y, point.x, radius=search_radius,
                    keyword=keyword, typ=typ)

