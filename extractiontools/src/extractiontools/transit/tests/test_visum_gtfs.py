#!/usr/bin/env python
#coding:utf-8
# Author:   --<>
# Project: ELAN
# Purpose:
# Created: 14/03/2015

import sys
import unittest
import os

from extractiontools.transit.gtfs_visum import GTFSVISUM
import tempfile
import shutil


class TestGTFS2VISUM(unittest.TestCase):
    """Test Visum Netfile reader and writer"""

    @classmethod
    def setUpClass(cls):
        folder = os.path.dirname(__file__)
        cls.netfile = os.path.join(folder, 'Niebuell-Korridor.net')
        cls.gtfs_folder = tempfile.mkdtemp()
        if not os.path.exists(cls.gtfs_folder):
            os.makedirs(cls.gtfs_folder)
        cls.converter = GTFSVISUM(netfile=cls.netfile,
                                  gtfs_folder=cls.gtfs_folder)

    @classmethod
    def tearDownClass(cls):
        """Remove the temp folder"""
        shutil.rmtree(cls.gtfs_folder)

    def test_01_convert(self):
        self.converter.visum2gtfs()
        print( self.converter.gtfs)






if __name__=='__main__':
    unittest.main()