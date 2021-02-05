# -*- coding: utf-8 -*-
'''
***************************************************************************
    bahn_query.py
    ---------------------
    Date                 : October 2019
    Copyright            : (C) 2019 by Christoph Franke
    Email                : franke at ggr-planung dot de
***************************************************************************
*                                                                         *
*   This program is free software: you can redistribute it and/or modify  *
*   it under the terms of the GNU General Public License as published by  *
*   the Free Software Foundation; either version 3 of the License, or     *
*   (at your option) any later version.                                   *
*                                                                         *
***************************************************************************

scrapers for public stops and public transport connections
'''

import datetime
import time
import re
from lxml import html
import requests


class BahnQuery:
    '''
    Deutsche-Bahn-scraper for connections, stops and time tables
    '''
    base_url = 'https://reiseauskunft.bahn.de'
    reiseauskunft_url = f'{base_url}/bin/query.exe/dn'
    timetable_url = f'{base_url}/bin/bhftafel.exe/dn'
    mobile_url = 'https://mobile.bahn.de/bin/mobil/query.exe/dox'

    # default request parameters for connections
    reiseauskunft_params = {
        'start': 1,
        'S': '',
        'Z': '',
        'date': '',
        'time': ''
    }

    # default request parameters for public stops
    stop_params = {
        'Id': 9627,
        'n': 1,
        'rt': 1,
        'use_realtime_filter': 1,
        'performLocating': 2,
        'tpl': 'stopsnear',
        'look_maxdist': 2000,
        'look_stopclass': 1023,
        'look_x': 0,
        'look_y': 0
    }

    # default request parameters for timetables
    timetable_params = {
        'ld': 96242,
        'country': 'DEU',
        'rt': 1,
        'bt': 'dep',
        'start': 'yes',
        'productsFilter': 1111111111,
        'max': 10000,
        'maxJourneys': 10000,
        'time': '24:00',
        'date': '',
        'evaId': 0,
    }

    date_format = '%d.%m.%Y'
    time_format = '%H:%M'

    def __init__(self, dt=None, timeout=0):
        '''
        Parameters
        ----------
        dt : datetime.date, optional
            date to scrape data for, defaults to today
        timeout : int, optional
            pause between requests in seconds to avoid block due to too many
            requests, defaults to no pause
        '''
        dt = dt or datetime.date.today()
        self.date = dt
        self.timeout = timeout

    def _to_db_coord(self, c):
        return str(round(c * 1000000))

    def _from_db_coord(self, c):
        return c / 1000000.

    def stops_near(self, point, max_distance=2000, stopclass=1023, n=999999):
        '''
        get closest station to given point

        Parameters
        ----------
        point : tuple
            x, y values in WGS84 (4326)
        max_distance : int
            maximum distance of stops to given point
        stopclass : int
            id of internal DB stop class
        n : int
            maximum number of stops returned

        Returns
        -------
        list
            stops ordered by distance (ascending)
        '''
        # set url-parameters
        params = self.stop_params.copy()
        params['look_maxdist'] = max_distance
        params['look_stopclass'] = stopclass
        x, y = point
        params['look_x'] = self._to_db_coord(x)
        params['look_y'] = self._to_db_coord(y)

        r = requests.get(self.mobile_url, params=params, verify=False)

        root = html.fromstring(r.content)
        rows = root.xpath('//a[@class="uLine"]')

        def parse_href_number(tag, href):
            regex = f'{tag}=(\d+)!'
            return re.search(regex, href).group(1)

        stops = []
        for row in rows:
            name = row.text
            if not name:
                continue
            href = row.attrib['href']
            x = int(parse_href_number('X', href))
            y = int(parse_href_number('Y', href))
            id = int(parse_href_number('id', href))
            dist = int(parse_href_number('dist', href))
            stop = {
                'x': self._from_db_coord(x),
                'y': self._from_db_coord(y),
                'name': name,
                'distance': dist,
                'id': id,
                'epsg': 4326
            }
            stops.append(stop)

            # response should be sorted by distances in first place,
            # but do it again because you can
        stops_sorted = sorted(stops, key=lambda x: x['distance'])
        if n < len(stops_sorted):
            stops = stops_sorted[:n]

        return stops

    def fastest_route(self, origin_name, destination_name, times, max_retries=1):
        '''
        scrape fastest connection by public transport between origin and
        destination

        Parameters
        ----------
        origin_name : str
            address or station name to depart from
        destination_name : str
            address or station name to arrive at
        times : list of int or list of str
            departure times (e.g. [14, 15, 16] or [14:00, 15:00, 16:00])
        max_retries : int
            maximum number of retries per time slot if DB api is returning
            valid results

        Returns
        -------
        tuple
            duration in minutes, departure time as text, number of changes,
            modes as text
        '''
        params = self.reiseauskunft_params.copy()
        params['date'] = self.date.strftime(self.date_format)
        params['S'] = origin_name
        params['Z'] = destination_name

        duration = float("inf")
        departure = mode = ''
        changes = 0

        def request_departure_table(t):
            params['time'] = t
            r = requests.get(self.reiseauskunft_url, params=params,
                             verify=False)
            root = html.fromstring(r.content)
            if root.xpath('//div[contains(@class, "errorMessage")]'):
                raise ConnectionError
            try:
                table = root.get_element_by_id('resultsOverview')
            except KeyError:
                return
            return table

        for t in times:
            retries = 0
            table = None
            while retries <= max_retries:
                try:
                    table = request_departure_table(t)
                    break
                except ConnectionError:
                    time.sleep(self.timeout)
                    print('retry')
                    retries += 1

            # still no table -> skip
            if not table:
                print('skip')
                continue

            rows = table.xpath('//tr[@class="firstrow"]')

            for row in rows:
                # duration
                content = row.find_class('duration')
                h, m = content[0].text.replace('\n', '').split(':')
                d = int(h) * 60 + int(m)
                # if already found shorter duration -> skip
                if d >= duration:
                    continue
                duration = d

                # departure
                content = [t.text for t in row.find_class('time')]

                matches = re.findall(
                    r'\d{1,2}:\d{1,2}', ' - '.join(content))
                departure = matches[0] if len(matches) > 0 else ''

                # modes
                content = row.find_class('products')
                mode = content[0].text.replace('\n', '')

                # changes
                content = row.find_class('changes')
                changes = int(content[0].text.replace('\n', ''))

            time.sleep(self.timeout)

        return duration, departure, changes, mode

    def scrape_journeys(self, stop_id: int, max_journeys: int = 10000) -> list:
        '''
        scrape journeys from time table for stop with given id (HAFAS)

        Parameters
        ----------
        stop_ids: int
            HAFAS id of the stop
        max_journeys : int, optional
            maximum number of routes per requested time table

        Returns
        -------
        list
            journeys with their attributes as dictionaries
        '''
        # set url-parameters
        params = self.timetable_params.copy()
        params['date'] = self.date.strftime(self.date_format)
        params['maxJourneys'] = max_journeys
        params['evaId'] = stop_id
        r = requests.get(self.timetable_url, params=params, verify=False)
        root = html.fromstring(r.content)
        rows = root.xpath('//tr')
        journeys = [row for row in rows
                    if row.get('id') and 'journeyRow_' in row.get('id')]
        error = root.xpath('//div[contains(@class, "error")]')
        if error:
            if getattr(error, 'text', None):
                raise ConnectionError(
                    f'Error while querying DB-site: {r.url} : "{error.text}')
            else:
                raise ConnectionError(
                    f'Error while querying DB-site: {r.url}')
        res = []
        for journey in journeys:
            j_attrs = {}

            route = journey.find_class('route')[0]
            destination = route.find('.//a').text.replace('\n', '')
            j_attrs['destination'] = destination.strip()
            route_times = re.findall(
                r'\d{1,2}:\d{1,2}', route.text_content())
            dt_txt = route_times[0]
            dt = datetime.datetime.strptime(dt_txt, self.time_format).time()
            j_attrs['departure'] = datetime.datetime.combine(self.date, dt)
            at_txt = route_times[-1]
            at = datetime.datetime.strptime(at_txt, self.time_format).time()
            j_attrs['arrival'] = datetime.datetime.combine(self.date, at)
            # arrival time is before departure time > next day
            # ToDo: are there trips longer than one day? If so, we have to
            # look through all route times
            if at < dt:
                j_attrs['arrival'] += datetime.timedelta(days=1)
            # there are 2 train tds, the 2nd contains the train info
            train = journey.find_class('train')[-1]
            train_txt = train.text_content()
            # train number in brackets
            train_nr_b = re.findall(r'\([0-9]+\)', train_txt)
            train_nr_b = train_nr_b[0] if train_nr_b else ''
            # rest of string is the name of the train
            train_name = train_txt.replace(train_nr_b, '')
            # remove line returns and white spaces
            j_attrs['name'] = ' '.join(train_name.split()).strip()
            j_attrs['number'] = train_nr_b.replace('(', '').replace(')', '')
            # url is relative
            j_attrs['url'] = self.base_url + train.find('a').attrib['href']
            res.append(j_attrs)
        return res

    def scrape_route(self, url):
        route = []
        r = requests.get(url)
        root = html.fromstring(r.content)
        content = root.xpath('//div[@id="content"]')[0]
        #dt_div = content.find_class('trainroute')[0]
        #date_txt = re.findall(r'\d{1,2}.\d{1,2}.\d{2,4}', dt_div.text)[0]
        #date = datetime.datetime.strptime(date_txt, '%d.%m.%y').date()

        table = content.xpath('.//table[contains(@class, "result")]')[0]
        rows = table.findall('tr')

        def parse_time(div):
            txt = re.findall(r'\d{1,2}:\d{1,2}', div.text)
            if not txt:
                return
            t = datetime.datetime.strptime(txt[0], self.time_format).time()
            return t

        for row in rows:
            cls = row.get('class')
            if not (cls and cls.startswith('trainrow')):
                continue
            section = {}
            station = row.find_class('station')[0].find('a')
            section['station_name'] = station.text
            station_url = station.attrib['href']
            section['station_id'] = re.findall('input=.*%23([0-9]+)',
                                               station_url)[0]
            # arrival date (resp. departure date, if no arrival time) can be
            # retrieved from station url
            date_txt = re.findall('date=([0-9\.]+)', station_url)[0]
            date = datetime.datetime.strptime(date_txt, '%d.%m.%y').date()

            arr_time = parse_time(row.find_class('arrival')[0])
            dep_time = parse_time(row.find_class('departure')[0])

            if arr_time:
                section['arrival'] = datetime.datetime.combine(
                    date, arr_time)
            if dep_time:
                # departure is next day
                if arr_time and arr_time > dep_time:
                    date += datetime.timedelta(days=1)
                section['departure'] = datetime.datetime.combine(
                    date, dep_time)
            route.append(section)
        return route

    def n_departures(self, stop_ids, max_journeys=10000):
        '''
        scrape number of departures for stops with given ids (HAFAS)

        Parameters
        ----------
        stop_ids : list
            HAFAS ids of stops
        max_journeys : int, optional
            maximum number of routes per requested time table
        '''
        # set url-parameters
        params = self.timetable_params.copy()
        params['date'] = self.date
        params['maxJourneys'] = max_journeys
        n_departures = []

        for id in stop_ids:
            params['evaId'] = id
            r = requests.get(self.timetable_url, params=params, verify=False)
            root = html.fromstring(r.content)
            rows = root.xpath('//tr')
            journeys = [row for row in rows
                        if row.get('id') and 'journeyRow_' in row.get('id')]
            n_departures.append(len(journeys))
            time.sleep(self.timeout)

        return n_departures

    def get_timetable_url(self, stop_id):
        '''
        set up an url to request the stop with given id
        '''
        params = self.timetable_params.copy()
        params['date'] = self.date.strftime(self.date_format)
        params['evaId'] = stop_id
        args = [f'{v}={k}' for v, k in params.items()]
        url = f'{self.timetable_url}?{"&".join(args)}'
        return url

    def get_routes(self, stop_id, stop_name):
        pass

