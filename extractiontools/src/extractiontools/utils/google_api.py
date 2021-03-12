import requests
import time


class GooglePlacesAPI:
    url = 'https://maps.googleapis.com/maps/api/place'
    nearby_url = f'{url}/nearbysearch/json'
    types = [
    'accounting', 'airport', 'amusement_park', 'aquarium', 'art_gallery',
    'atm', 'bakery', 'bank', 'bar', 'beauty_salon', 'bicycle_store',
    'book_store', 'bowling_alley', 'bus_station', 'cafe', 'campground',
    'car_dealer', 'car_rental', 'car_repair', 'car_wash', 'casino', 'cemetery',
    'church', 'city_hall', 'clothing_store', 'convenience_store', 'courthouse',
    'dentist', 'department_store', 'doctor', 'drugstore', 'electrician',
    'electronics_store', 'embassy', 'fire_station', 'florist', 'funeral_home',
    'furniture_store', 'gas_station', 'gym', 'hair_care', 'hardware_store',
    'hindu_temple', 'home_goods_store', 'hospital', 'insurance_agency',
    'jewelry_store', 'laundry', 'lawyer', 'library', 'light_rail_station',
    'liquor_store', 'local_government_office', 'locksmith', 'lodging',
    'meal_delivery', 'meal_takeaway', 'mosque', 'movie_rental', 'movie_theater',
    'moving_company', 'museum', 'night_club', 'painter', 'park', 'parking',
    'pet_store', 'pharmacy', 'physiotherapist', 'plumber', 'police',
    'post_office', 'primary_school', 'real_estate_agency', 'restaurant',
    'roofing_contractor', 'rv_park', 'school', 'secondary_school', 'shoe_store',
    'shopping_mall', 'spa', 'stadium', 'storage', 'store', 'subway_station',
    'supermarket', 'synagogue', 'taxi_stand', 'tourist_attraction',
    'train_station', 'transit_station', 'travel_agency', 'university',
    'veterinary_care', 'zoo'
    ]

    def __init__(self, key, logger=None):
        self.key = key
        self.logger = logger

    def query_places_nearby(self, lat: float, lon: float, radius: int=5000,
                            keyword: str=None, typ: int=None):
        '''max-radius 50000'''

        params = {
            'key': self.key,
            'radius': 1000,
            'language': 'de',
            'location': f'{lat},{lon}',
        }
        if typ and typ in self.types:
            params['type'] = typ
        if keyword:
            params['keyword'] = keyword

        r = requests.get(self.nearby_url, params=params)
        if r.status_code != 200:
            raise Exception('Error while requesting Google API')
        res_j = r.json()
        if res_j['status'] == 'REQUEST_DENIED':
            raise Exception(r['error_message'])
        if res_j['status'] not in ['OK', 'ZERO_RESULTS'] and self.logger:
            self.logger.warning(f"({lat}, {lon}) {r['error_message']}")
        results = res_j['results']
        next_page_token = res_j.get('next_page_token')
        # pagination
        while next_page_token:
            time.sleep(1)
            r = requests.get(self.nearby_url,
                             params={'key': self.key,
                                     'pagetoken': next_page_token})
            res_j = r.json()
            if res_j['status'] == 'INVALID_REQUEST':
                print()
                if res_j['status'] not in ['OK', 'ZERO_RESULTS'] and self.logger:
                    self.logger.warning(f"({lat}, {lon}) {r['error_message']}")
            next_page_token = res_j.get('next_page_token')
            results.extend(res_j['results'])
        if len(results) >= 60 and self.logger:
            self.logger.warning(
                f'60 results found at ({lat}, {lon}). There might be more '
                'to be found. Google Places caps at 60 results')
        return results
