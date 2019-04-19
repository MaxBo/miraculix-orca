#!/usr/bin/env python
#coding:utf-8

import sys
import unittest
import os

from extractiontools.transit.gtfs import GTFS
import tempfile
import shutil


class TestGTFS(unittest.TestCase):
    """Test GTFS"""
    @classmethod
    def setUpClass(cls):
        folder = tempfile.mkdtemp()
        if not os.path.exists(folder):
            os.makedirs(folder)
        cls.gtfs = GTFS(folder)

    @classmethod
    def tearDownClass(cls):
        """Remove the temp folder"""
        shutil.rmtree(cls.gtfs.folder)

    def test_01_dtypes(self):
        calendar = self.gtfs.calendar
        print(calendar.header)
        calendar.add_rows(1)
        row = calendar.rows[0]
        row.service_id = 1
        row.monday = 1
        row.tuesday = 1
        row.wednesday = 1
        row.thursday = 1
        row.friday = 1
        row.saturday = 1
        row.sunday = 1
        row.start_date = '20000101'
        row.end_date = '20201231'
        print(calendar)
        print(calendar.start_date)

    def test_03_test_defaults(self):
        agency = self.gtfs.agency
        agency.add_rows(3)
        print(agency)

    def test_02_write_table(self):
        if os.path.exists(self.gtfs.path):
            os.remove(self.gtfs.path)
        calendar = self.gtfs.calendar
        calendar.write_file()

    def test_03_read_table(self):
        calendar = self.gtfs.calendar
        data = calendar.read_file()
        print(data)

    def test_04_unicode(self):
        """Test Unicode reading and writing"""
        agency = self.gtfs.agency
        agency.add_rows(1)
        row = agency.rows[0]
        row.agency_id = 1
        row.agency_name = u'ÄÖÜß'
        print(agency)
        agency.write_file()

    def test_05_read_unicode(self):
        agency = self.gtfs.agency
        data = agency.read_file()
        print(data)

    def test_06_read_gtfs(self):
        folder = os.path.dirname(__file__)
        filename = 'gtfs_klein2.zip'
        gtfs = GTFS(folder, filename)
        gtfs.read_tables()

        print(gtfs.calendar)
        cal = gtfs.calendar.rows[0]
        # test if values are read correctly
        assert cal.service_id == 1
        assert cal.monday == 11
        assert cal.tuesday == 22
        assert cal.saturday == 6
        # assert that the sunday is masked, because
        # the column is missing in the data
        assert cal.sunday.mask

        print(gtfs.agency)
        print(gtfs.stops)
        print(gtfs.routes)
        print(gtfs.trips)
        print(gtfs.transfers)



if __name__=='__main__':
    unittest.main()