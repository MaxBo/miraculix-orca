#!/usr/bin/env python
#coding:utf-8
# Author:   --<>
# Purpose:
# Created: 13/03/2015

from zipfile import ZipFile, ZIP_DEFLATED
from io import BytesIO


class ReadFileInZipfile(object):
    """File Handler to read a file from a zipfile"""
    def __init__(self, zipfilepath, mode, tablename):
        """
        Properties
        ----------
        zipfilename : str
            the path to the zipfile
        mode : str
            the filemode ('a' or 'w')
        """
        self.zipfilepath = zipfilepath
        if 'r' not in mode:
            raise AssertionError('mode must contain r')
        self.mode = mode
        self.tablename = tablename

    def __enter__(self):
        self.zf = ZipFile(self.zipfilepath,
                          mode=self.mode,
                          compression=ZIP_DEFLATED)
        self.f = self.zf.open(self.tablename, self.mode)
        return self.f

    def __exit__(self, t, value, traceback):
        self.f.close()
        self.zf.close()


class WriteFileInZipfile(object):
    """File Handler to a write file within a zipfile"""
    def __init__(self, zipfilepath, mode, tablename):
        """
        Properties
        ----------
        zipfilename : str
            the path to the zipfile
        mode : str
            the filemode ('a' or 'w')
        """
        self.zipfilepath = zipfilepath
        if mode not in 'aw':
            raise AssertionError('mode must be a or w to allow writing')
        self.mode = mode
        self.tablename = tablename

    def __enter__(self):
        self.buf = BytesIO()
        return self.buf

    def __exit__(self, t, value, traceback):
        with ZipFile(self.zipfilepath,
                     mode=self.mode,
                     compression=ZIP_DEFLATED) as zf:
            self.buf.seek(0)
            data = self.buf.read()
            zf.writestr(self.tablename, data)
            self.buf.close()
