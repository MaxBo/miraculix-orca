#coding:utf-8

import numpy as np
import datetime

def to_datetime(val):
    """convert time given as string in format HH:MM:SS to datetime object"""
    val_split = val.split(':')
    hrs = int(val_split[0])
    if hrs >= 24:
        # should days be handled somehow?
        days = (hrs // 24) + 1
        hours = hrs % 24
        val_split[0] = '%02d' % hours
        val = ':'.join(val_split)
        #dt = datetime.datetime.strptime(val, '%d-%H:%M:%S')
    dt = datetime.datetime.strptime(val, '%H:%M:%S')
    ms = dt.isoformat()
    return ms
np_to_datetime = np.vectorize(to_datetime)


def to_hhmmss(hrs, minutes, secs):
    """
    convert arrays with hours, minutes and seconds to string in format
    HH.MM:SS. Hours after 23:59:59 will be kept as 24:XX:XX, 25:XX:XX etc.
    """
    a = '%02d:%02d:%02d'
    return a % (hrs, minutes, secs)
np_to_hhmmss = np.vectorize(to_hhmmss)


def timedelta_to_HHMMSS(td_arr):
    """
    convert timedelta-array to a string in format
    HH.MM:SS. Hours after 23:59:59 will be kept as 24:XX:XX, 25:XX:XX etc.
    """
    arr = td_arr.astype(int)
    hrs = arr // 3600
    minutes = np.mod(arr, 3600) / 60
    secs = np.mod(arr, 60)
    return np_to_hhmmss(hrs, minutes, secs)


def get_timedelta_from_arr(arr):
    """
    get timedelta array from an array of datetimes
    """
    base = np_to_datetime('00:00:00').astype('M8[s]')
    date = np_to_datetime(arr).astype('M8[s]')
    return date - base
