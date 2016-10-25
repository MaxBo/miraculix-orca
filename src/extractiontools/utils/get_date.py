#!/usr/bin/env python
#coding:utf-8


import datetime
import time
import pytz


berlin = pytz.timezone('Europe/Berlin')


class Date(datetime.date):
    """date with today as default value for year, month and day"""
    def __new__(cls, year=None, month=None, day=None):
        """
        Parameters
        ----------
        year : int, optional
        month : int, optional
        day : int, optional
        """
        today = datetime.date.today()
        if year is None:
            year = today.year
        if month is None:
            month = today.month
        if day is None:
            day = today.day
        return super(Date, cls).__new__(cls, int(year), int(month), int(day))

    def __repr__(self):
        """string representation """
        return format(self, '%d.%m.%y')

    def __str__(self):
        """"""
        return repr(self)

    @classmethod
    def from_string(cls, datestring):
        """Create a Date-instance from a datestring in format DD.MM.YYYY"""
        if not datestring:
            return Date.__new__(cls)
        try:
            day, month, year = (int(x) for x in datestring.split('.'))
        except ValueError as e:
            msg = '{} not valid for format DD.MM.YYYY'.format(datestring)

            raise ValueError(msg)
        return super(Date, cls).__new__(cls, year, month, day)

    def get_timestamp(self, time_to_convert):
        """
        convert a date to a timestamd
        """
        time_format = '%H:%M:%S'
        dateString = format(self, '%Y-%m-%d')
        try:
            time_str = time.strftime(time_format, time_to_convert)
            return '%s %sCEST' %(dateString, time_str)
        except:
            return None

    def shift_day(self, days=1):
        """adds or substracts the number of days"""
        new_date = Date.fromordinal(self.toordinal() + days)
        return new_date

    @property
    def day(self):
        return self.timetuple().tm_mday

def get_timestamp2(time_to_convert):
    t = time_to_convert

    if t:
        dt = datetime.datetime(t.tm_year,
                               t.tm_mon,
                               t.tm_mday,
                               t.tm_hour,
                               t.tm_min,
                               t.tm_sec)

        # check Sommerzeit
        local = berlin.localize(dt, is_dst=False)
        tz = 'CEST' if local.dst() else 'CET'
        fmt = '%Y-%m-%d %H:%M:%S{tz}'.format(tz=tz)

        return dt.strftime(fmt)
