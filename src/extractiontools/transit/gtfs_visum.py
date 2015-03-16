#!/usr/bin/env python
#coding:utf-8
# Author:   --<>
# Purpose:
# Created: 13/03/2015


from extractiontools.transit.gtfs import GTFS
from extractiontools.transit.visum import Visum


net_types_map={'Rail': 2,
               'Bus': 3,
               'AST': 6,
               'Sonstiges': 4,}

class GTFSVISUM(object):

    def __init__(self, netfile, gtfs_folder, gtfs_filename='gtfs.zip'):
        self.visum = Visum(netfile)
        self.gtfs = GTFS(gtfs_folder, gtfs_filename)

    @classmethod
    def net2gtfs(cls, options):
        netfile = options.netfile
        gtfs_filename = options.gtfs
        if options.gtfs_folder:
            gtfs_folder = options.gtfs_folder
        else:
            gtfs_folder = os.path.dirname(netfile)
        self = cls(netfile, gtfs_folder, gtfs_filename)
        return self

    def visum2gtfs(self):
        self.visum.read_tables()
        self.convert_calendar()
        self.convert_betreiber()

    def convert_calendar(self):
        visum_vt = self.visum.verkehrstag
        gtfs_cal = self.gtfs.calendar
        n_rows = visum_vt.n_rows
        gtfs_cal.add_rows(n_rows)
        row = gtfs_cal.rows[0]
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

    def convert_betreiber(self):
        visum_betreiber = self.visum.betreiber
        gtfs_agency = self.gtfs.agency
        n_rows = visum_betreiber.n_rows
        gtfs_agency.add_rows(n_rows)
        gtfs_agency.rows.agency_id = visum_betreiber.rows.NR
        gtfs_agency.rows.agency_name = visum_betreiber.rows.NAME



def main():
    from argparse import ArgumentParser
    parser = ArgumentParser(description='convert VISUM .net-file to gfts-feed')
    parser.add_argument("-d", "--debug", action="store_true",
                        help="print debug information",
                        dest="debug", default=False)
    parser.add_argument("-p", "--proj_code",
                        help="set the corresponding proj-init code for the coordinates inside the .net file. See http://code.google.com/p/pyproj/source/browse/trunk/lib/pyproj/data/epsg for all possiblities. If option is not set WGS84 will be used.",
                        dest="proj_code",
                        default="4326")
    parser.add_argument("-n", "--net", help='full path of the VISUM .net file',
                        dest='netfile', type=str)

    parser.add_argument("-f", "--gtfs-folder",
                        help='folder of the gtfs-file to produce. If path is relative, use folder of .net file',
                        dest='gtfs_folder', type=str)

    parser.add_argument("-g", "--gtfs",
                        help='path of the gtfs-file to produce. If path is relative, use folder of .net file',
                        dest='gtfs', type=str, default='gtfs.zip')

    options = parser.parse_args()

    try:
        ntg = NetToGtf(options, net_types_map={'Rail': 2,
                                               'Bus': 3,
                                               'AST': 6,
                                               'Sonstiges': 4,},
                                calendar_types=None)

        ntg.write_gtf()
    except InvalidInputException:
        print u"Error: looks like the input file is not valid!\n"
        parser.print_help()
        exit(-1)
    #except:
        print u"something went wrong\n"
        parser.print_help()
        exit(-1)

if __name__ == '__main__':
    main()