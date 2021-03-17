import orca
import math
import json
from orcadjango.decorators import meta
from extractiontools.ausschnitt import Extract
from extractiontools.connection import Connection
from extractiontools.utils.google_api import GooglePlacesAPI


@meta(group='(7) Google', required='create_db')
@orca.step()
def google_places(database: str, google_key: str, places_table: str,
                  places_type: str, places_keyword: str,
                  project_area: 'ogr.Geometry', places_search_radius:  int):
    '''
    Search for places with Google Places. Places are defined within the Google
    Places API as establishments, geographic locations, or prominent points of
    interest. Points will be distributed to cover the whole project area with
    the set search radius.
    The API will be queried at each point. Each query can return 60 places
    at max. Set a smaller search radius if you
    expect a lot of places with the set keyword/type combination.
    '''
    google = GoogleApp(database, boundary=project_area)
    google.get_places(google_key, keyword=places_keyword, typ=places_type,
                      table=places_table, search_radius=places_search_radius)


class GoogleApp(Extract):
    '''
    Collection of queries for the Google API. Store results in a database.
    '''
    schema = 'google'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.set_target_boundary(self.boundary)
        self.update_boundaries()

    def get_places(self, key, search_radius=5000, keyword=None, typ=None,
                   table='places'):
        '''
        Query the google API at points in the project area. The points
        will be distributed to cover the whole project area with the given
        search radius per point.
        '''
        #point_distance = math.floor(search_radius * math.sqrt(2))

        with Connection(login=self.login) as conn:
            sql = f'''
            DROP TABLE IF EXISTS {self.schema}.{table};
            CREATE TABLE {self.schema}.{table} (
              place_id TEXT PRIMARY KEY,
              name TEXT,
              vicinity TEXT,
              geom GEOMETRY,
              types TEXT,
              opening_hours JSON,
              business_status TEXT,
              rating DOUBLE PRECISION,
              user_ratings_total INTEGER,
              icon TEXT,
              photos JSON
            )
            '''
            self.logger.debug(sql)
            cursor = conn.cursor()
            cursor.execute(sql)

            # ToDo: table comments

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
            cursor.execute(sql, {'boundary_name': self.boundary_name})
            points = cursor.fetchall()

            n_found = 0
            for i, point in enumerate(points):
                api = GooglePlacesAPI(key, logger=self.logger)
                results = api.query_places_nearby(
                    point.y, point.x, radius=search_radius,
                    keyword=keyword, typ=typ)
                n_found += len(results)
                for result in results:
                    location = result['geometry']['location']
                    types = ','.join(result.get('types', []))
                    sql = f'''
                    INSERT INTO {self.schema}.{table}
                    (place_id, name, vicinity,
                    geom,
                    types, opening_hours, business_status,
                    rating, user_ratings_total,
                    icon, photos)
                    VALUES (
                    %(place_id)s, %(name)s, %(vicinity)s,
                    ST_TRANSFORM(ST_SetSRID(ST_MakePoint({location['lng']}, {location['lat']}), 4326), {self.target_srid}),
                    %(types)s, %(opening_hours)s, %(business_status)s,
                    {result.get('rating')}, {result.get('user_ratings_total')},
                    %(icon)s, %(photos)s
                    )
                    ON CONFLICT (place_id) DO NOTHING;
                    '''
                    cursor.execute(sql, {
                        'place_id': result['place_id'],
                        'name': result.get('name'),
                        'vicinity': result.get('vicinity'),
                        'types': types,
                        'opening_hours': json.dumps(
                            result.get('opening_hours', {})),
                        'business_status': result.get('business_status'),
                        'icon': result.get('icon'),
                        'photos': json.dumps(
                            result.get('photos', {}))
                    })

                if i > 0 and i % 50 == 0:
                    self.logger.info(f'{i+1}/{len(points)} processed. '
                                     f'{n_found} places found so far.')
                    conn.commit()


