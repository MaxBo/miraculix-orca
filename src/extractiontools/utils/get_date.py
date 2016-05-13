#!/usr/bin/env python
#coding:utf-8


import datetime
import time

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
        return super(Date, cls).__new__(cls, year, month, day)

    def __repr__(self):
        """string representation """
        return format(self, '%d.%m.%y')

    def __str__(self):
        """"""
        return repr(self)

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

def get_timestamp2(time_to_convert):
    if time_to_convert:
        return time.strftime('%Y-%m-%d %H:%M:%SCEST', time_to_convert)
