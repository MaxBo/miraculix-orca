#coding:utf-8

from pyproj import Proj, transform
from collections import defaultdict
import numpy as np



class Projections(dict):
    def __init__(self, *args, **kwargs):
        super(Projections, self).__init__(*args, **kwargs)
        # Shere_Mercator used in WMS-Servers, so that VISUM exports netfile
        # in this projection by default
        Sphere_Mercator_epsg_53004 = '+proj=merc +lat_ts=0 +lon_0=0 +k=1.000000 +x_0=0 +y_0=0 +a=6371000 +b=6371000 +units=m'
        self[53004] = Proj(Sphere_Mercator_epsg_53004)

    def __getitem__(self, key):
        """If not found, return a new projection by default"""
        try:
            return super(Projections, self).__getitem__(key)
        except KeyError:
            return Proj(init='epsg:{}'.format(key))


class Transform(object):
    """Mixin Class to transform xy to latlon"""
    def transform_to_latlonh(self, xcol='XKOORD', ycol='YKOORD', zcol=None,
                            to_epsg=4326, from_epsg=53004):
        x = getattr(self.rows, xcol)
        y = getattr(self.rows, ycol)
        if zcol is None:
            z = np.zeros(x.shape, x.dtype)
        else:
            z = getattr(self.rows, zcol)
        projections = Projections()
        from_proj = projections[from_epsg]
        to_proj = projections[to_epsg]
        lon, lat, h = transform(from_proj, to_proj, x, y, z)
        return lat, lon, h

    def transform_to_latlon(self, xcol='XKOORD', ycol='YKOORD',
                            to_epsg=4326, from_epsg=53004):
        lat, lon, h = self.transform_to_latlonh(xcol=xcol,
                                                ycol=ycol,
                                                zcol=None,
                                                to_epsg=to_epsg,
                                                from_epsg=from_epsg)
        return lat, lon

    def calc_lat_lon(self, xcol='XKOORD', ycol='YKOORD',
                     to_epsg=4326, from_epsg=53004):
        """set self.lat and self.lon from XKoord and YKoord"""
        self.lat, self.lon = self.transform_to_latlon(xcol, ycol,
                                                      to_epsg, from_epsg)
