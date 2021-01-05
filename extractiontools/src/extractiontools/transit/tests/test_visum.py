#!/usr/bin/env python
#coding:utf-8
# Author:   --<>
# Project: ELAN
# Purpose:
# Created: 14/03/2015

import sys
import unittest
import os

from extractiontools.transit.visum import Visum
import tempfile
import shutil
from numpy.testing import assert_array_less, assert_array_equal


class TestVisum(unittest.TestCase):
    """Test Visum Netfile reader and writer"""

    @classmethod
    def setUpClass(cls):
        folder = os.path.dirname(__file__)
        netfile = os.path.join(folder, 'Niebuell-Korridor.net')
        cls.visum = Visum(netfile)

    def test_01_read(self):
        visum = self.visum
        #print visum
        visum.read_tables()
        print(visum)

    def test_02_transform(self):
        visum = self.visum
        lat, lon, h = visum.knoten.transform_to_latlon()
        assert_array_less(lon, 10, 'lon should be < 10')
        assert_array_less(8, lon, 'lon should be > 8')
        assert_array_less(lat, 56, 'lat should be < 56')
        assert_array_less(54, lat, 'lat should be > 54')
        assert_array_equal(h, 0, 'h should be 0')

    def test_03_write(self):
        visum = self.visum
        folder = os.path.dirname(visum.netfile)
        visum.netfile = os.path.join(folder, 'test.net')
        visum.write_tables()





if __name__=='__main__':
    unittest.main()