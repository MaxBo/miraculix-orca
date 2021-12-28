# -*- coding: utf-8 -*-

from osgeo import gdal
import os
import numpy as np
from osgeo.gdalconst import *

from elan.agents.raster2 import Grids
from simcommon.matrixio import XArray, Aufwand, XRecArray


def main():
    folder = r'E:\GGR\Berlin Dichte\30 Gis\31 gisserver_backup\tiffs'
    path = os.path.join(folder, 'ew_ha_raster.tiff')

    # lies Eingangsdaten als Array
    f = gdal.Open(path)
    val = f.ReadAsArray()

    # erzeuge Kernel
    size = 1
    x = np.arange(-size, size + 1)
    y = np.arange(-size, size + 1)
    xx, yy = np.meshgrid(x, y)
    # Distanz zum Kernel-Mittelpunkt
    dist = np.sqrt(xx**2 + yy**2)
    # Distance Decay-Parameter
    beta = -1
    weights = np.exp(beta * dist)

    # Erzeuge Grids-Objekt
    g = Grids(1886, 2030, 100, -100, 4464400, 3374900, max_rings=3)

    # Gewichtung der Bezugsflächen 1
    freq = np.ones(val.shape)
    # only include inhabited raster cells into Bezugsflächen
    freq[val == 0] = 0

    # berechner Kernel
    g.init_array('result', val.shape, default=0)
    g.set_array('data_d', val, val.shape)
    g.set_array('frequencies', freq, freq.shape)
    g.set_weights(weights)
    g.calc_moving_window()

    # setze Ergebnis auf 0 auf unbesiedelten Rasterzellen
    # (Dichte der Eingangsdaten = 0)
    g.result[val == 0] = 0

    # Erzeuge Ergebnis-Tiff
    driver = f.GetDriver()
    outDs = driver.Create(os.path.join(folder, 'yyyy.tiff'),
                          1886, 2030, 1, GDT_Float64)
    outBand = outDs.GetRasterBand(1)
    outBand.WriteArray(g.result, 0, 0)
    outBand.FlushCache()
    outBand.SetNoDataValue(0)
    outDs.SetGeoTransform(f.GetGeoTransform())
    outDs.SetProjection(f.GetProjection())
    del outDs


def classify_ew_dichte():
    print('hallo')
    folder = r'E:\GGR\Berlin Dichte\30 Gis\31 gisserver_backup\tiffs'
    path = os.path.join(folder, 'ew_ha_raster.tiff')

    # lies Eingangsdaten als Array
    f = gdal.Open(path)
    ew = f.ReadAsArray().astype('d')

    path = os.path.join(folder, 'bundeslaender.tiff')
    f = gdal.Open(path)
    bl = f.ReadAsArray()
    berlin = bl == 11
    umland = bl != 11

    ew_berlin = ew * berlin
    ew_umland = ew * umland
    bins = np.concatenate([np.arange(0, 510, 10), [1200]])

    aufwand_berlin = Aufwand(ew_berlin, ew_berlin)
    verteilung_berlin = aufwand_berlin.Entfernungsklassen(bins)

    aufwand_umland = Aufwand(ew_umland, ew_umland)
    verteilung_umland = aufwand_umland.Entfernungsklassen(bins)
    ra = XRecArray.fromarrays((bins[1:], verteilung_berlin, verteilung_umland),
                              names=('dichte', 'berlin', 'umland'))
    for row in ra:
        print(row.dichte, ',', row.berlin, ',', row.umland)


if __name__ == '__main__':
    # main()
    classify_ew_dichte()
