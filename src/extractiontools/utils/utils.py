#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Author Tobias Ottenweller
# 21.10.2010 - 03.12.2010
# Gertz Gutsche Rümenapp Gbr


import copy
import sqlite3
import math
import datetime
from pyproj import Proj, transform

#from graphserver.core import Street


def read_config(filename, defaults, raise_exception=False):
    try:
        config = copy.copy(defaults)

        f = open(filename)

        for line in f:
            if line[0] not in ( '#', '\n' ) or line == '\r\n':
                line = line.replace('\r\n', '').replace('\n', '')
                stuff = line.split('=')

                if stuff[0] in config:
                    config[stuff[0]] = stuff[1]
    except:
        if raise_exception:
            raise
        else:
            print('ERROR: could not read configuration')
            return defaults

    return config


""" Adds two times (format: HH:MM:SS) together and returns the result.
    May return hours greater then 23.
"""
def time_adder(time_string1, time_string2):
    ( h1, m1, s1 ) = time_string1.split(':')
    ( h2, m2, s2 ) = time_string2.split(':')
    h = int(h1) + int(h2)
    m = int(m1) + int(m2)
    s = int(s1) + int(s2)
    while s > 59: s -= 60; m += 1
    while m > 59: m -= 60; h += 1

    return '%02d:%02d:%02d' % (h, m, s)


""" Converts an integer representing a number of seconds into a human readable time string (H:MM:SS).
"""
def seconds_time_string(seconds):
    seconds = int(seconds)

    hours = 0
    minutes = 0

    while seconds >= 3600:
        hours += 1
        seconds -= 3600

    while seconds >= 60:
        minutes += 1
        seconds -= 60

    return '%d:%02d:%02d' % (hours, minutes, seconds)





def string_to_datetime(s):
    ''' Converts a string with a format like this DD:MM:YYYY:HH:MM into a datetime object
    '''
    sl = [int(x) for x in s.split(':')]
    return datetime.datetime(sl[2],sl[1], sl[0], sl[3], sl[4])


''' Transforms given coordinates to WGS84. from_proj needs to be a Proj object,
    x, y and z need to be strings.
'''
def coord_to_wgs84(from_proj, x, y, z='0'):
    wgs84 = Proj(proj='latlon', datum='WGS84')

    x = float(x.replace(',', '.'))
    y = float(y.replace(',', '.'))
    z = float(z.replace(',', '.'))

    return transform(from_proj, wgs84, x, y, z)


''' Function will delete blank lines in a given text-file.
    Note: it loads the whole file into memory.
'''
def eliminate_blank_lines(filename, eol_character):
    lines = []

    with open(filename, 'r') as file:
        for l in file:
            if not l == eol_character:
                lines.append(l)

    with open(filename, 'w') as file:
        for l in lines:
            file.write(l)


''' Returns the distance between two WGS84 coordinates in meters.
    Original code from http://www.johndcook.com/python_longitude_latitude.html
'''
def distance(lat1, long1, lat2, long2):
    degrees_to_radians = math.pi/180.0

    phi1 = (90.0 - lat1)*degrees_to_radians
    phi2 = (90.0 - lat2)*degrees_to_radians

    theta1 = long1*degrees_to_radians
    theta2 = long2*degrees_to_radians

    cos = (math.sin(phi1)*math.sin(phi2)*math.cos(theta1 - theta2) + math.cos(phi1)*math.cos(phi2))
    arc = math.acos( cos )

    return arc * 6373000 # result are meters