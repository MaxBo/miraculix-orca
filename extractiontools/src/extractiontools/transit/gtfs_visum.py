#!/usr/bin/env python
#coding:utf-8
# Author:   --<>
# Purpose:
# Created: 13/03/2015


from extractiontools.transit.gtfs import GTFS
from extractiontools.transit.visum import Visum
from extractiontools.transit.time_utils import (get_timedelta_from_arr,
                                                timedelta_to_HHMMSS, )
import numpy as np
from simcommon.matrixio import XRecArray, XMaskedRecarray
import os
import logging

net_types_map={'Rail': 2,
               'Bus': 3,
               'AST': 6,
               'Sonstiges': 4,}
def get_nettype(key):
    return net_types_map.get(key, -1)


class GTFSVISUM(object):

    def __init__(self, netfile, gtfs_folder, gtfs_filename='gtfs.zip',
                 logger=None):
        """
        convert a visum netfile into a gtfs zipfile
        """
        self.logger = logger or logging.getLogger(__name__)
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
        self.logger.info('read visum tables from {}'.format(self.visum.path))
        self.visum.read_tables()
        self.logger.info('convert calendar')
        self.convert_calendar()
        self.logger.info('convert betreiber')
        self.convert_betreiber()
        self.logger.info('convert knoten')
        self.convert_knoten()

        self.logger.info('convert stops')
        self.convert_stops()
        self.logger.info('convert gehzeiten')
        self.convert_gehzeiten()

        self.logger.info('convert linienrouten')
        self.convert_linienrouten()
        self.logger.info('convert routes')
        self.convert_routes()

        self.logger.info('convert stop_times')
        self.convert_stop_times()
        self.logger.info('write gtfs tables to {}'.format(self.gtfs.path))
        self.gtfs.write_tables()
        self.logger.info('finished writing')

    def convert_calendar(self):
        visum_vt = self.visum.verkehrstag
        gtfs_cal = self.gtfs.calendar
        n_rows = visum_vt.n_rows
        gtfs_cal.add_rows(n_rows)
        gtfs_cal.calc_start_date()

    def convert_betreiber(self):
        visum_betreiber = self.visum.betreiber
        gtfs_agency = self.gtfs.agency
        n_rows = visum_betreiber.n_rows
        gtfs_agency.add_rows(n_rows)
        gtfs_agency.rows.agency_id = visum_betreiber.rows.NR
        gtfs_agency.rows.agency_name = visum_betreiber.rows.NAME

    def convert_knoten(self):
        knoten = self.visum.knoten
        knoten.calc_lat_lon()

    def convert_stops(self):
        """convert haltestellen"""
        gtfs_stops = self.gtfs.stops
        hp = self.visum.haltepunkt
        lre = self.visum.linienroutenelement
        unique_hp = np.unique(lre.HPUNKTNR)
        hp.is_angefahren = np.in1d(hp.NR, unique_hp, assume_unique=True)

        hp_angefahren = hp.rows[hp.is_angefahren]
        self.logger.debug('{} of {} hp angefahren'.format(len(hp_angefahren), hp.n_rows))

        gtfs_stops.add_rows(len(hp_angefahren))
        knoten = self.visum.knoten
        hp_knoten_lat = knoten.get_rows_by_pkey('lat', hp_angefahren.KNOTNR)
        hp_knoten_lon = knoten.get_rows_by_pkey('lon', hp_angefahren.KNOTNR)

        #lat, lon = visum_haltepunkt.transform_to_latlon()
        gtfs_stops.rows.stop_id = np.array('S', dtype='U1').view(np.chararray) + hp_angefahren.NR.astype('U49')
        gtfs_stops.rows.stop_name = hp_angefahren.NAME
        gtfs_stops.rows.stop_lat = hp_knoten_lat
        gtfs_stops.rows.stop_lon = hp_knoten_lon
        # station gets an 'S' in front of the station number
        hp_hstber = gtfs_stops.rows.parent_station
        station = (
            np.full_like(hp_hstber, 'S') +
            hp_angefahren.HSTBERNR.astype(hp_hstber.dtype))
        hp_hstber[:] = station

    def convert_gehzeiten(self):
        """convert Uebergangsgehzeithstber"""
        gz = self.visum.uebergangsgehzeithstber
        tf = self.gtfs.transfers

        # check if stops exists
        stops = self.gtfs.stops
        dtype = stops.stop_id.dtype

        hstber_prefix = np.array('S', dtype='U1').view(np.chararray)

        vhstber = hstber_prefix + gz.VONHSTBERNR.astype('U49')
        von_hp, vh_in_stops = stops.get_dictlist_by_non_unique_key(stops.parent_station,
                                                                  'stop_id', vhstber)

        nhstber = hstber_prefix + gz.NACHHSTBERNR.astype('U49')
        #gz.NACHHSTBERNR.astype(dtype)
        nach_hp,nh_in_stops = stops.get_dictlist_by_non_unique_key(stops.parent_station,
                                                                   'stop_id', nhstber)

        in_stops = vh_in_stops & nh_in_stops

        result = []
        transfer_type = tf.defaults['transfer_type']
        min_transfer_times = gz.ZEIT
        for i in range(gz.n_rows):
            if in_stops[i]:
                min_transfer_time = min_transfer_times[i]
                for von_stop in von_hp[i]:
                    for nach_stop in nach_hp[i]:
                        row = (von_stop, nach_stop,
                               transfer_type, min_transfer_time)
                        result.append(row)

        data = XRecArray(result, dtype=tf.cols.dtype)
        unique_data = np.unique(data)
        n_rows = len(unique_data)


        tf.add_rows(n_rows)
        tf.rows[:] = unique_data

    def convert_routes(self):
        """convert routes"""
        gtfs_routes = self.gtfs.routes
        visum_linie = self.visum.linie
        betreiber = self.visum.betreiber
        gtfs_routes.add_rows(visum_linie.n_rows)
        gtfs_routes.rows.route_id = visum_linie.rows.NAME

        # agency
        betreiber_id = visum_linie.rows.BETREIBERNR
        gtfs_routes.rows.agency_id = betreiber_id

        dt = gtfs_routes.rows.route_short_name.dtype
        gtfs_routes.rows.route_short_name = visum_linie.rows.NAME.astype(dt)
        gtfs_routes.rows.route_long_name = visum_linie.rows.NAME

        # map VSYSCODE vectorized
        line_vsyscode = visum_linie.rows.VSYSCODE
        vsyscode =  self.visum.vsys.rows.CODE
        vsysname = self.visum.vsys.rows.NAME
        d = dict(list(zip(vsyscode, vsysname)))
        def get_gtfs_name(key):
            name = d.get(key, -1)
            gtfs_type = net_types_map.get(name, -1)
            return gtfs_type
        mp = np.vectorize(get_gtfs_name)
        gtfs_vsyscode = np.ma.masked_equal(mp(line_vsyscode), -1)
        gtfs_routes.rows.route_type = gtfs_vsyscode

    def convert_linienrouten(self):
        """convert linienrouten to trips, stop_times and shapes"""
        lre = self.visum.linienroutenelement
        visum_lr = self.visum.linienroute
        lr, lr_idx, shape_id,lr_counts = np.unique(
            lre.get_columns_by_names(visum_lr.pkey_cols),
            return_index=True,
            return_counts=True,
            return_inverse=True,
        )
        visum_lr.add_rows(len(lr))
        visum_lr.rows.data = lr
        visum_lr.lr_idx = lr_idx
        visum_lr.lr_counts = lr_counts
        visum_lr.shape_id = np.arange(visum_lr.n_rows)
        lre.shape_id = shape_id


        fzpe = self.visum.fahrzeitprofilelement
        visum_fp = self.visum.fahrzeitprofil
        fp, fzpe_indx, fzpe_counts = np.unique(
            fzpe.get_columns_by_names(visum_fp.pkey_cols),
            return_index=True,
            return_counts=True)
        visum_fp.add_rows(len(fp))
        visum_fp.rows.data = fp
        visum_fp.fzpe_indx = fzpe_indx
        visum_fp.fzpe_counts = fzpe_counts

    def convert_stop_times(self):
        """Stop times"""
        knoten = self.visum.knoten
        haltepunkt = self.visum.haltepunkt
        fahrten = self.visum.fahrplanfahrt
        fp = self.visum.fahrzeitprofil
        fzpe = self.visum.fahrzeitprofilelement
        linie = self.visum.linie
        lr = self.visum.linienroute
        lre = self.visum.linienroutenelement

        self.logger.debug('add trips')
        trips = self.gtfs.trips
        stoptimes = self.gtfs.stoptimes

        # trips
        trips.add_rows(fahrten.n_rows)
        # give new trip id as sequence
        trips.rows.trip_id = fahrten.NR
        trips.rows.route_id = fahrten.LINNAME

        fzpe_profil_cols = fzpe.get_columns_by_names_hashable(lr.pkey_cols)
        fzpe_rowidx_in_lre = lr.get_rows_by_pkey('lr_idx',
                                                   fzpe_profil_cols)
        fzpe_counts_in_lre = lr.get_rows_by_pkey('lr_counts',
                                                   fzpe_profil_cols)


        fahrt_profil_cols = fahrten.get_columns_by_names_hashable(fp.pkey_cols)
        fahrt_rowidx_in_fzpe = fp.get_rows_by_pkey('fzpe_indx',
                                                  fahrt_profil_cols)
        fahrt_counts_in_fzpe = fp.get_rows_by_pkey('fzpe_counts',
                                                   fahrt_profil_cols)

        self.logger.debug('add shapes')
        # get the unique shapes defined by lr and vonlreidx -> tolreidx

        fahrt_von_fzpe_cols = fahrten.get_columns_by_names_hashable(fp.pkey_cols+
                                                                    ['VONFZPELEMINDEX'])
        fahrten_von_lridx = fzpe.get_rows_by_pkey('LRELEMINDEX',
                                                   fahrt_von_fzpe_cols)

        fahrt_nach_fzpe_cols = fahrten.get_columns_by_names_hashable(fp.pkey_cols+
                                                                    ['NACHFZPELEMINDEX'])
        fahrten_nach_lridx = fzpe.get_rows_by_pkey('LRELEMINDEX',
                                                fahrt_nach_fzpe_cols)

        fahrt_lr_cols = [getattr(fahrten, col) for col in lr.pkey_cols]
        shape_idx = XRecArray.fromarrays(
            fahrt_lr_cols + [fahrten_von_lridx, fahrten_nach_lridx],
            names=['LINNAME', 'LINROUTENAME', 'RICHTUNGCODE',
                   'VONLRIDX', 'NACHLRIDX'])

        #sh, sh_idx, fahrt_shape_ids, lr_counts = np.unique(
        sh, sh_idx, fahrt_shape_rows, lr_counts = np.unique(
            shape_idx,
            return_index=True,
            return_counts=True,
            return_inverse=True,
        )
        fahrt_shape_ids = fahrt_shape_rows + 1
        shape_count_knoten = sh.NACHLRIDX - sh.VONLRIDX + 1
        shapes = self.gtfs.shapes
        shapes.add_rows(shape_count_knoten.sum())

        knoten = self.visum.knoten
        lre = self.visum.linienroutenelement
        lre_knoten = lre.KNOTNR

        lat = knoten.get_rows_by_pkey('lat', lre_knoten)
        lon = knoten.get_rows_by_pkey('lon', lre_knoten)

        shape_lr_key_col = sh[lr.pkey_cols]
        shape_lr_key_col = shape_lr_key_col.view(dtype='S%s' %shape_lr_key_col.itemsize)
        shape_lr_idx = lr.get_rows_by_pkey(
            'lr_idx', shape_lr_key_col)

        # loop over shapes
        sh_von_idx = 0
        for shape_row, shape in enumerate(sh):
            shape_id = shape_row + 1
            n_knoten = shape_count_knoten[shape_row]
            sh_nach_idx = sh_von_idx + n_knoten
            rows = shapes.rows[sh_von_idx:sh_nach_idx]
            rows.shape_id = shape_id
            rows.shape_pt_sequence = np.arange(1, n_knoten + 1)

            lr_idx_start = (shape_lr_idx[shape_row] - 1) + shape.VONLRIDX
            lr_idx_end = shape_lr_idx[shape_row] + shape.NACHLRIDX

            rows.shape_pt_lat = lat[lr_idx_start:lr_idx_end]
            rows.shape_pt_lon = lon[lr_idx_start:lr_idx_end]

            # next shape
            sh_von_idx = sh_nach_idx

        # set trip shape_id
        trips.rows.shape_id = fahrt_shape_ids

        self.logger.debug('add stop_times')
        # stop times
        n_rows = (fahrten.NACHFZPELEMINDEX - fahrten.VONFZPELEMINDEX + 1).sum()

        stoptimes.add_rows(n_rows)


        st_von_idx = 0

        # loop over fahrten
        for f, fahrt in enumerate(fahrten.rows):
            if not f % 500:
                self.logger.debug('{}: fahrt {}'.format(f, fahrt))
            start_idx = fahrt_rowidx_in_fzpe[f]
            counts = fahrt_counts_in_fzpe[f]
            end_idx = start_idx + counts
            st_nach_idx = st_von_idx + counts
            fahrt_st = stoptimes.rows[st_von_idx:st_nach_idx]

            fzp = fzpe.rows[start_idx:end_idx]
            fahrt_fzpe = fzp[fahrt.VONFZPELEMINDEX - 1 :
                             fahrt.NACHFZPELEMINDEX]

            fahrt_lre_idx_start = fzpe_rowidx_in_lre[start_idx]

            fahrt_st.trip_id = fahrt.NR

            # get absolute time from visum relative times
            ankunft = get_timedelta_from_arr(fahrt_fzpe.ANKUNFT.filled())
            abfahrt = get_timedelta_from_arr(fahrt_fzpe.ABFAHRT.filled())
            fahrt_abfahrt = get_timedelta_from_arr(fahrt.ABFAHRT)

            arrival = fahrt_abfahrt + ankunft
            departure = fahrt_abfahrt + abfahrt
            fahrt_st.arrival_time = timedelta_to_HHMMSS(arrival)
            fahrt_st.departure_time = timedelta_to_HHMMSS(departure)
            current_index = fahrt_fzpe.LRELEMINDEX + (fahrt_lre_idx_start - 1)
            fahrt_st.stop_sequence = lre.INDEX.take(current_index)
            fahrt_st.stop_id = np.array('S', dtype='U1').view(np.chararray) + lre.HPUNKTNR.take(current_index).astype('U49')
            fahrt_st.pickup_type = (fahrt_fzpe.EIN == 0)
            fahrt_st.drop_off_type = (fahrt_fzpe.AUS == 0)

            # next trip
            st_von_idx = st_nach_idx


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

    gtfs2visum = GTFSVISUM.net2gtfs(options)
    gtfs2visum.visum2gtfs()

if __name__ == '__main__':
    main()