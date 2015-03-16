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
        print visum

    def test_02_write(self):
        visum = self.visum
        folder = os.path.dirname(visum.netfile)
        visum.netfile = os.path.join(folder, 'test.net')
        visum.write_tables()




if __name__=='__main__':
    unittest.main()