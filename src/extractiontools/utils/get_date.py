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
        convert a date to a timestamp
        """
        return get_timestamp2(time_to_convert, date=self)

    def shift_day(self, days=1):
        """adds or substracts the number of days"""
        new_date = Date.fromordinal(self.toordinal() + days)
        return new_date

    @property
    def day(self):
        return self.timetuple().tm_mday

def get_timestamp2(time_to_convert, date=None):
    """
    get a timestamp for the time_to_convert

    Parameters
    ----------
    time_to_convert : datetime-object
    date : Date-instance (optional)

    if date is None, then use the date of the time_to_convert,
    otherwise use the date provided by `date' together with
    the time provided by `time_to_convert'

    Returns
    -------
    timestamp : str
    """
    t = time_to_convert
    if not t:
        return ''

    if date is None:
        date = Date(year = t.tm_year,
                    month = t.tm_mon,
                    day = t.tm_mday)

    dt = datetime.datetime(date.year,
                           date.month,
                           date.day,
                           t.tm_hour,
                           t.tm_min,
                           t.tm_sec)

    # check Sommerzeit
    local = berlin.localize(dt, is_dst=False)
    tz = 'CEST' if local.dst() else 'CET'
    fmt = '%Y-%m-%d %H:%M:%S{tz}'.format(tz=tz)

    return dt.strftime(fmt)
