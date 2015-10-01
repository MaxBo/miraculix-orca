#!/usr/bin/env python
#coding:utf-8


import datetime

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
