#!/usr/bin/env python
# -*- coding: utf-8 -*-

#
# Author:   Tobias Ottenweller
# Date:     11.8.2010 - 12.11.2010
#
# Gertz Gutsche Rümenapp Gbr
#

import re
from zipfile import ZipFile
import os
import sys
import codecs
import csv
from pyproj import Proj, transform

from extractiontools.utils.utf8csv import UnicodeWriter
from extractiontools.utils.utils import read_config, time_adder, coord_to_wgs84, eliminate_blank_lines

pd = '+proj=merc +lat_ts=0 +lon_0=0 +k=1.000000 +x_0=0 +y_0=0 +a=6371000 +b=6371000 +units=m'
# TODO: - error handling
#       - documenting
#       - read config file


class InvalidInputException(Exception):
    pass


class NetToGtf():
    """
    convert VISUM Net-File to GTFS.zip
    """

    def __init__(self, options,
                 net_types_map={}, calendar_types=None):
        self.debug = options.debug
        self.net_file = options.netfile
        gtfs = options.gtfs
        if not os.path.isabs(gtfs):
            folder = os.path.dirname(self.net_file)
            gtfs = os.path.join(folder, gtfs)
        self.output_file = os.extsep.join((os.path.splitext(gtfs)[0], 'zip'))
        if os.path.exists(self.output_file):
            os.remove(self.output_file)
        #self.from_proj = Proj(init='epsg:{epsg}'.format(epsg=options.proj_code))
        self.from_proj = Proj(pd)
        self.net_route_types_map = net_types_map
        self.calendar_types = calendar_types
        self.table_to_func_mapper = {
            '$VERKEHRSTAG':self._write_calendar,
            '$VSYS':self._process_route_types,
            '$BETREIBER':self._write_agency,
            '$HALTESTELLE': self._write_stations,
            '$HALTESTELLENBEREICH': self._write_stops,
            '$HALTEPUNKT':self._process_stop_points,
            '$LINIE':self._write_routes,
            '$LINIENROUTENELEMENT':self._process_stop_id_mapper,
            #'$FAHRZEITPROFILELEMENT':self._process_fzp_stop_id_mapper,
            '$FAHRZEITPROFILELEMENT':self._process_raw_stop_times,
            '$FAHRPLANFAHRT':self._write_stop_times_and_trips,
            '$UEBERGANGSGEHZEITHSTBER':self._write_tranfers,
            '$KNOTEN':self._process_vertices,
        }


        self._check_input()
        eliminate_blank_lines(self.net_file, self.eol)

    def _write_calendar(self, table_header_line): # reads from $VERKEHRSTAG and ???
        # TODO: at this moment I'm not sure how to deal with calendar.txt
        #       so there will be just a single daily entry!

        calendar_header = ( u'service_id', u'monday', u'tuesday', u'wednesday', u'thursday',
                            u'friday', u'saturday', u'sunday', u'start_date', u'end_date' )

        calendar_daily_entry = ( '1', '1', '1', '1', '1', '1', '1', '1', '20000101', '20201231' )

        f = open('calendar.txt', 'w')
        writer = UnicodeWriter(f, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)

        writer.writerow(calendar_header)
        writer.writerow(calendar_daily_entry)
        f.close()

        try:
            zip_file = ZipFile(self.output_file, 'a')
            zip_file.write('calendar.txt')
        finally:
            zip_file.close()

        os.remove('calendar.txt')
        return ''


    def _process_route_types(self, table_header_line): # reads from $VSYS
        ''' Creates a dictionary [route_types_map] to map between .net file's $VSYS CODE
            and in gtfs specified route-types codes. It uses information read in from the
            config file [net_route_types_mapper]. If none information given a default value
            (3/Bus) will be set.
        '''
        if self.debug: print( u'processing route_types')

        self.route_types_map = {}
        columns = table_header_line.split(':')[1].split(';')

        name_column = columns.index('NAME')
        code_column = columns.index('CODE')

        while True:
            line = self.get_line()
            if line.startswith('$'):  # next section
                break
            entry = line.split(';')
            self.route_types_map[entry[code_column]] = self.net_route_types_map.get(entry[name_column], '3')

        if self.debug: print( u'route_types_map: %s' % self.route_types_map)
        return line



    def _write_agency(self, table_header_line): # reads form $BETREIBER
        f = open('agency.txt', 'w')
        writer = UnicodeWriter(f, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        writer.writerow(( u'agency_id', u'agency_name', u'agency_url', u'agency_timezone' )) # header

        columns = table_header_line.split(':')[1].split(';')

        id_column = columns.index('NR')
        name_column = columns.index('NAME')


        # read and write the entries
        while True:
            line = self.get_line()
            if line.startswith('$'):  # next section
                break
            entry = line.split(';')
            writer.writerow((entry[id_column], entry[name_column], u'http://www.example.com', u'Europe/Berlin'))
        f.close()

        try:
            zip_file = ZipFile(self.output_file, 'a')
            zip_file.write('agency.txt')
        finally:
            zip_file.close()

        os.remove('agency.txt')
        return line

    def _process_vertices(self, table_header_line):
        self.vertices = {}
        columns = table_header_line.split(':')[1].split(';')

        id_column = columns.index('NR')
        x_coord_column = columns.index('XKOORD')
        y_coord_column = columns.index('YKOORD')

        while True:
            line = self.get_line()
            if line.startswith('$'):  # next section
                break
            entry = line.split(';')

            lat, lon, h = coord_to_wgs84(self.from_proj,
                                         entry[x_coord_column],
                                         entry[y_coord_column])

            self.vertices[entry[id_column]] = ( str(lon), str(lat) )
        return line


    def _write_stations(self, table_header_line): # reads from $HALTESTELLE
        self.stations = []
        columns = table_header_line.split(':')[1].split(';')

        id_column = columns.index('NR')
        name_column = columns.index('NAME')
        x_coord_column = columns.index('XKOORD')
        y_coord_column = columns.index('YKOORD')


        # read and write the entries
        while True:
            line = self.get_line()
            if line.startswith('$'):  # next section
                break
            entry = line.split(';')

            (lat, lon, h) = coord_to_wgs84(self.from_proj,
                                           entry[x_coord_column],
                                           entry[y_coord_column])
            stop_name = entry[name_column] if entry[name_column] else 'Unbenannter Stop'

            self.stations.append(( 'S'+entry[id_column],
                                   stop_name,
                                   str(lon),
                                   str(lat),
                                   '1',
                                   '' ))
        return line



    def _write_stops(self, table_header_line): # reads from $HALTESTELLENBEREICH
        f = open('stops.txt', 'w')
        writer = UnicodeWriter(f,
                               delimiter=',',
                               quotechar='"',
                               quoting=csv.QUOTE_MINIMAL)
        writer.writerow(( u'stop_id',
                          u'stop_name',
                          u'stop_lat',
                          u'stop_lon',
                          u'location_type',
                          u'parent_station' )) # header

        writer.writerows(self.stations)

        columns = table_header_line.split(':')[1].split(';')

        id_column = columns.index('NR')
        name_column = columns.index('NAME')
        vertex_id_column = columns.index('KNOTNR')
        station_id_column = columns.index('HSTNR')
        x_coord_column = columns.index('XKOORD')
        y_coord_column = columns.index('YKOORD')


        # read and write the entries
        while True:
            line = self.get_line()
            if line.startswith('$'):  # next section
                break
            entry = line.split(';')

            (lat, lon, h) = coord_to_wgs84(self.from_proj,
                                           entry[x_coord_column],
                                           entry[y_coord_column])
            stop_name = entry[name_column] if entry[name_column] else 'Unbenannter Stop'

            writer.writerow(( entry[id_column],
                              stop_name,
                              str(lon),
                              str(lat),
                              '0',
                              'S'+entry[station_id_column] ))

        f.close()

        try:
            zip_file = ZipFile(self.output_file, 'a')
            zip_file.write('stops.txt')
        finally:
            zip_file.close()

        os.remove('stops.txt')
        return line

    def _process_stop_points(self, table_header_line): # rads from $HALTEPUNKT
        ''' Creates a dictionary [vertex_to_stop_mapper] that maps between vertex-IDs and stops-IDs.
        '''
        self.vertex_to_stop_mapper = {}
        self.stop_point_to_stop_mapper = {}

        columns = table_header_line.split(':')[1].split(';')

        vertex_id_column = columns.index('KNOTNR')
        stop_id_column = columns.index('HSTBERNR')
        stop_point_id_column = columns.index('NR')


        while True:
            line = self.get_line()
            if line.startswith('$'):  # next section
                break
            entry = line.split(';')

            self.vertex_to_stop_mapper[entry[vertex_id_column]] = entry[stop_id_column]
            self.stop_point_to_stop_mapper[entry[stop_point_id_column]] = entry[stop_id_column]
        return line


    def _write_routes(self, table_header_line): # reads form $LINIE
        f = open('routes.txt', 'a')
        writer = UnicodeWriter(f, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        writer.writerow(( u'route_id', u'agency_id', u'route_short_name', u'route_long_name', u'route_type' ))# header

        columns = table_header_line.split(':')[1].split(';')
        for i,stuff in enumerate(columns): # find the position of the required columns
            if stuff == 'NAME':
                name_column = id_column = i
            elif stuff == 'VSYSCODE':
                route_type_column = i
            elif stuff == 'BETREIBERNR':
                agency_id_column = i

        while True:
            line = self.get_line()
            if line.startswith('$'):  # next section
                break
            entry = line.split(';')
            writer.writerow(( entry[id_column],
                              '1',
                              entry[name_column],
                              '',
                              self.route_types_map[entry[route_type_column]] ))
        f.close()

        try:
            zip_file = ZipFile(self.output_file, 'a')
            zip_file.write('routes.txt')
        finally:
            zip_file.close()

        os.remove('routes.txt')
        return line


    def _process_stop_id_mapper(self, table_header_line): # reads from $LINIENROUTENELEMENT
        ''' This method provides a dictionary to map from 'LRELEMINDEX' keys in the
            '$LINIENROUTENELEMENT' table to actual 'stop_id's.
            It contains a dictionary for each trip, which maps betwen indexes and stop_ids.
            See 'mapping net to gtf.txt' for further information.
        '''
        self.stop_id_mapper = {}
        self.shapes = {}

        columns = table_header_line.split(':')[1].split(';')
        for i,stuff in enumerate(columns): # find the position of the required columns
            if stuff == 'HPUNKTNR':
                stop_id_column = i
            elif stuff == 'LINNAME':
                route_id_column = i
            elif stuff == 'LINROUTENAME':
                lr_id_column = i # not actual the lr_id (see 'mapping net to gtf.txt')
            elif stuff == 'RICHTUNGCODE':
                direction_column = i
            elif stuff == 'INDEX':
                index_column = i
            elif stuff == 'ISTROUTENPUNKT':
                is_stop_column = i
            elif stuff == 'KNOTNR':
                vertex_id_column = i

        dict_lr_id = lre_dict = None
        while True:
            line = self.get_line()
            if line.startswith('$'):  # next section
                break
            entry = line.split(';')

            lr_id = '_'.join([entry[route_id_column], entry[lr_id_column], entry[direction_column]])

            # add entry to the shapes
            if lr_id not in self.shapes:
                self.shapes[lr_id] = [ ( entry[index_column], entry[vertex_id_column] ) ]
            else:
                self.shapes[lr_id].append(( entry[index_column], entry[vertex_id_column] ))

            if not entry[is_stop_column] == '0' and entry[stop_id_column]: # entry is a stop
                if not lr_id == dict_lr_id:
                    self.stop_id_mapper[dict_lr_id] = lre_dict
                    lre_dict = {}
                    dict_lr_id = lr_id

                lre_dict[entry[index_column]] = self.stop_point_to_stop_mapper[entry[stop_id_column]]

        self.stop_id_mapper[dict_lr_id] = lre_dict
        return line

    def _process_raw_stop_times(self, table_header_line): # reads from $FAHRZEITPROFILELEMENT
        if self.debug: print( 'processing raw stop times')
        self.fzp_stop_id_mapper = {}
        dict_fzp_id = fzpe_dict = None

        columns = table_header_line.split(':')[1].split(';')

        route_id_column = columns.index('LINNAME')
        lrname_column = columns.index('LINROUTENAME')
        direction_column = columns.index('RICHTUNGCODE')
        fzprofilname_column = columns.index('FZPROFILNAME')
        arrival_time_column = columns.index('ANKUNFT')
        departure_time_column = columns.index('ABFAHRT')
        stop_column = columns.index('LRELEMINDEX') # not stop_id (see 'mapping net to gtf.txt')
        stop_sequence_column = columns.index('INDEX')


        self.raw_stop_times = {}
        while True:
            line = self.get_line()
            if line.startswith('$'):  # next section
                break
            entry = line.split(';')


            rst_id = '_'.join([entry[route_id_column], entry[lrname_column], entry[direction_column]])
            fzp_id = '_'.join([entry[route_id_column], entry[lrname_column], entry[direction_column], entry[fzprofilname_column]])

            if self.debug: print( rst_id, fzp_id)


            if not fzp_id == dict_fzp_id:
                self.fzp_stop_id_mapper[dict_fzp_id] = fzpe_dict
                fzpe_dict = {}
                dict_fzp_id = fzp_id

            fzpe_dict[entry[stop_sequence_column]] = self.stop_id_mapper[rst_id][entry[stop_column]]


            lre = self.stop_id_mapper[rst_id]
            #fzpe = self.fzp_stop_id_mapper[fzp_id]
            if entry[stop_column] in lre:
                stop_id = lre[entry[stop_column]]
                if fzp_id in self.raw_stop_times:
                    self.raw_stop_times[fzp_id].append(( entry[stop_sequence_column], stop_id, entry[arrival_time_column], entry[departure_time_column] ))
                else:
                    self.raw_stop_times[fzp_id] = [ ( entry[stop_sequence_column], stop_id, entry[arrival_time_column], entry[departure_time_column] ) ]

        if self.debug: print(self.raw_stop_times)
        return line

    def _write_stop_times_and_trips(self, table_header_line): # reads from $FAHRPLANFAHRT
        if self.debug: print( 'writing stop_times.txt and trips.txt')

        st_file = open('stop_times.txt', 'w')
        st_writer = UnicodeWriter(st_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        st_writer.writerow(( u'trip_id', u'arrival_time', u'departure_time', u'stop_id', u'stop_sequence' )) # header

        t_file = open('trips.txt', 'w')
        t_writer = UnicodeWriter(t_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        t_writer.writerow(( u'route_id', u'service_id', u'trip_id', u'shape_id' ))

        columns = table_header_line.split(':')[1].split(';')

        lr_id_column = columns.index('NR')
        departure_column = columns.index('ABFAHRT')
        route_id_column = columns.index('LINNAME')
        lrname_column = columns.index('LINROUTENAME')
        direction_column = columns.index('RICHTUNGCODE')
        fzprofilname_column = columns.index('FZPROFILNAME')

        lr_id = 0
        while True:
            line = self.get_line()
            if line.startswith('$'):  # next section
                break
            entry = line.split(';')

            lr_id += 1
            rst_id = '_'.join([entry[route_id_column], entry[lrname_column], entry[direction_column]])
            fzp_id = '_'.join([entry[route_id_column], entry[lrname_column], entry[direction_column], entry[fzprofilname_column]])
            fzp = self.raw_stop_times[fzp_id]
            if self.debug: print( 'writing trip: %s' % lr_id)

            t_writer.writerow(( entry[route_id_column],
                                '1',
                                str(lr_id),
                                fzp_id if fzp_id in self.shapes else u'' )) # TODO: service_id

            for stop in fzp:
                arrival_time = time_adder(stop[2], entry[departure_column])
                departure_time = time_adder(stop[3], entry[departure_column])
                st_writer.writerow(( str(lr_id), arrival_time, departure_time, stop[1], stop[0] ))

        st_file.close()
        t_file.close()

        try:
            zip_file = ZipFile(self.output_file, 'a')
            zip_file.write('stop_times.txt')
            zip_file.write('trips.txt')
        except:
            raise 'hallo'
        finally:
            zip_file.close()

        os.remove('stop_times.txt')
        os.remove('trips.txt')
        return line


    def _write_tranfers(self, table_header_line): # reads from $UEBERGANGSGEHZEITHSTBER
        if self.debug: print( 'writing tranfers')
        try:

            with open('transfers.txt', 'w') as f:
                writer = UnicodeWriter(f, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                writer.writerow(( u'from_stop_id', u'to_stop_id', u'transfer_type', u'min_transfer_time' ))

                columns = table_header_line.split(':')[1].split(';')
                columns[-1] = columns[-1].split(self.eol)[0]

                from_stop_id_column = columns.index('VONHSTBERNR')
                to_stop_id_column = columns.index('NACHHSTBERNR')
                time_column = columns.index('ZEIT')

                while True:
                    line = self.get_line()
                    if line.startswith('$'):  # next section
                        break
                    entry = line.split(';')

                    transfer_time = entry[time_column][:-3] if len(self.eol) == 2 else entry[time_column][:-2]
                    writer.writerow(( entry[from_stop_id_column], entry[to_stop_id_column], '2', transfer_time ))

        finally:
            try:
                zip_file = ZipFile(self.output_file, 'a')
                zip_file.write('transfers.txt')
            finally:
                zip_file.close()

            os.remove('transfers.txt')
        return line


    def _write_shapes(self):
        f = open('shapes.txt', 'w')
        writer = UnicodeWriter(f, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        writer.writerow(( u'shape_id', u'shape_pt_lat', u'shape_pt_lon', u'shape_pt_sequence' ))

        for s_id in self.shapes:
            for entry in self.shapes[s_id]:
                s_seq = entry[0]
                lat, lon  = self.vertices[entry[1]]

                writer.writerow(( s_id, lat, lon, s_seq ))

        f.close()

        try:
            zip_file = ZipFile(self.output_file, 'a')
            zip_file.write('shapes.txt')
        finally:
            zip_file.close()

        os.remove('shapes.txt')


    def _check_input(self):
        try:
            f = codecs.open(self.net_file, encoding='latin-1')
            # find out the end-of-line character
            l = f.next()
            if l[-2:] == u'\r\n': # windows (CRLF
                self.eol = u'\r\n'
            elif l[-1:] == u'\n': # unix (LF)
                self.eol = u'\n'
            else: # let's not expect more then LF and CRLF
                raise InvalidInputException()

            if self.debug: print( u'line ending: %s' % list(self.eol)) # in a list - so it won't cause a linebreak

            # TODO add more tests
        except:
            if self.debug: print( u'ERROR: compatibility test failed!')
            raise InvalidInputException()



    def write_gtf(self):
        with codecs.open(self.net_file, encoding='latin-1') as input_file:
            self.input_file = input_file

            try:
                current_line = self.get_line()
                while True:
                    # current_line is a table header
                    if current_line.startswith('$'):
                        table_name = current_line.split(':')[0]
                        if table_name in self.table_to_func_mapper:
                            print( table_name)
                            current_line = self.table_to_func_mapper[table_name](current_line)
                        else:
                            current_line = self.get_line()
                    else:
                        current_line = self.get_line()

            except StopIteration:
                self._write_shapes()
                if self.debug: print( 'file completely read')

        #except:
         #   if self.debug: print 'error in write_gtf'
          #  raise InvalidInputException()
        #finally:
            #self.input_file.close()

    def get_line(self):
        current_line = self.input_file.next().strip()
        if not current_line or current_line.startswith('*'):
            current_line = self.get_line()
        return current_line


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
        print( u"Error: looks like the input file is not valid!\n")
        parser.print_help()
        exit(-1)
    #except:
        print( u"something went wrong\n")
        parser.print_help()
        exit(-1)

if __name__ == '__main__':
    main()