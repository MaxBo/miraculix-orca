#!/usr/bin/env python
#coding:utf-8

from argparse import ArgumentParser
from extractiontools.raster_from_points import Points2Raster


class Zensus2Raster(Points2Raster):
    """Convert census data to raster data"""

    def do_stuff(self):
        """
        define here, what to execute
        """
        self.ew2hectar()
        self.zensus2km2()
        self.export2tiff('ew_ha_raster')
        self.export2tiff('einwohner_km2_raster')
        self.export2tiff('hhgroesse_d_km2_raster')
        self.export2tiff('wohnfl_wohnung_km2_raster')

    def ew2hectar(self):
        """convert Einwohner to Raster"""

        # create view joining geometry and values
        sql = """
CREATE OR REPLACE VIEW
{schema}.ew_hectar_pnt_laea AS
SELECT
v.cellcode,
v.pnt_laea,
z.einwohner
FROM
{schema}.zensus_ew_hectar z,
{schema}.laea_vector_100 v
WHERE v.cellcode = z.id;
        """.format(schema=self.schema)
        self.run_query(sql)

        self.point2raster(
            point_feature='{}.ew_hectar_pnt_laea'.format(self.schema),
            geom_col='pnt_laea',
            value_col='einwohner',
            target_raster='{}.ew_ha_raster'.format(self.schema),
            pixeltype='16BSI',
            srid=3035,
            reference_raster='{}.laea_raster_100'.format(self.schema),
            raster_pkey='rid',
            raster_col='rast',
            band=1,
            noData=0,
            overwrite=True)

    def zensus2km2(self):
        """convert ZensusData to km2-Raster"""

        # create view joining geometry and values
        sql = """
CREATE OR REPLACE VIEW
{schema}.zensus_km2_pnt_laea AS
SELECT
v.cellcode,
v.pnt_laea,
z.einwohner,
z.alter_d,
z.unter18_a,
z.ab65_a,
z.auslaender_a,
z.hhgroesse_d,
z.leerstandsquote,
z.wohnfl_bew_d,
z.wohnfl_wohnung
FROM
{schema}.zensus_km2 z,
{schema}.laea_vector_1000 v
WHERE v.cellcode = z.id;
        """.format(schema=self.schema)
        self.run_query(sql)

        self.point2km2raster(column='einwohner', dtype='16BSI')
        self.point2km2raster(column='hhgroesse_d', dtype='64BF', noData=-1)
        self.point2km2raster(column='wohnfl_wohnung', dtype='64BF', noData=-1)

    def point2km2raster(self, column, dtype, noData=0):
        """
        create raster-layer on km2-level

        Parameters
        ----------
        column : str
            the column name
        dtype : str
            the data type
        noData : double, optional (Default=0)
            the noData Value
        """
        self.point2raster(
            point_feature='{}.zensus_km2_pnt_laea'.format(self.schema),
            geom_col='pnt_laea',
            value_col=column,
            target_raster='{s}.{c}_km2_raster'.format(s=self.schema,
                                                      c=column),
            pixeltype=dtype,
            srid=3035,
            reference_raster='{}.laea_raster_1000'.format(self.schema),
            raster_pkey='rid',
            raster_col='rast',
            band=1,
            noData=noData,
            overwrite=True)


if __name__ == '__main__':

    parser = ArgumentParser(description="Create Raster with Census Data")

    parser.add_argument("-n", '--name', action="store",
                        help="Name of destination database",
                        dest="destination_db", default='extract')

    parser.add_argument('--host', action="store",
                        help="host",
                        dest="host", default='localhost')
    parser.add_argument("-p", '--port', action="store",
                        help="port", type=int,
                        dest="port", default=5432)
    parser.add_argument("-U", '--user', action="store",
                        help="user", type=str,
                        dest="user", default='osm')
    parser.add_argument('--subfolder', action="store",
                        help="subfolder to store the tiffs", type=str,
                        dest="subfolder", default='tiffs')
    options = parser.parse_args()

    z2r = Zensus2Raster(options,
                        db=options.destination_db)
    z2r.set_login(host=options.host,
                  port=options.port,
                  user=options.user)
    z2r.run()
