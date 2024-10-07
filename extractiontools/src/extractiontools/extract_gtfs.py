import gtfs_kit as gk
import geopandas as gp
import pandas as pd
import numpy as np
import logging
import os
import math

# in km/h
TRANSFER_SPEED = 3
# surplus to every calc. transfer / minimum transfer time in minutes
ADD_TRANSFER_TIME = 2
# max distance for adding calculated transfers between stations in meters
TRANSFER_MAX_DISTANCE = 200

'''
0 - Tram, Streetcar, Light rail. Any light rail or street level system within a metropolitan area.
1 - Subway, Metro. Any underground rail system within a metropolitan area.
2 - Rail. Used for intercity or long-distance travel.
3 - Bus. Used for short- and long-distance bus routes.
4 - Ferry. Used for short- and long-distance boat service.
5 - Cable tram. Used for street-level rail cars where the cable runs beneath the vehicle (e.g., cable car in San Francisco).
6 - Aerial lift, suspended cable car (e.g., gondola lift, aerial tramway). Cable transport where cabins, cars, gondolas or open chairs are suspended by means of one or more cables.
7 - Funicular. Any rail system designed for steep inclines.
11 - Trolleybus. Electric buses that draw power from overhead wires using poles.
12 - Monorail. Railway in which the track consists of a single rail or a beam.
'''

# bulk assign ranges of numbers of extended types to simple types
ROUTE_TYPE_MAP = dict(list(zip(list(range(100, 118)), [2]*18)) +
                      list(zip(list(range(200, 210)), [3]*10)) +
                      list(zip(list(range(700, 717)), [3]*17)) +
                      list(zip(list(range(900, 907)), [0]*7)) +
                      list(zip(list(range(1300, 1307)), [6]*7)) +
                      list(zip(list(range(1500, 1508)), [3]*8)))

ROUTE_TYPE_MAP.update({
    400: 0,
    401: 1,
    402: 1,
    403: 0,
    404: 0,
    405: 0,
    800: 3,
    1000: 4,
    1100: 6,
    1200: 4,
    1400: 7,
    1700: 0,
    1702: 0,
})


class ExtractGTFS():

    def __init__(self,
                 project_area: 'ogr.Geometry',
                 gtfs_input: str,
                 out_path: str,
                 do_visum_postproc: bool = True,
                 do_transferprocessing: bool = True,
                 logger=None):
        """"""
        self.gtfs_input = gtfs_input
        self.out_path = out_path
        self.do_visum_postproc = do_visum_postproc
        self.do_transferprocessing = do_transferprocessing
        self.out_path = "D:\\Downloads"
        self.gtfs_input = os.path.join(
            self.out_path, '20240826_fahrplaene_gesamtdeutschland_gtfs.zip')
        self.gtfs_output = os.path.join(self.out_path, 'gtfs_clipped.zip')
        self.project_area = project_area
        self.logger = logger or logging.getLogger(self.__module__)

    def extract(self):
        wkt = self.project_area.ExportToWkt()
        self.logger.info(f'Lade Feed aus der GTFS-Datei {self.gtfs_input}')
        area = gp.GeoDataFrame(geometry=gp.GeoSeries.from_wkt([wkt], crs=4326))
        feed = gk.read_feed(self.gtfs_input, dist_units='km')
        self.logger.info(f'Beschneide Feed')
        clip = gk.miscellany.restrict_to_area(feed, area)
        del(feed)
        self.logger.info('Entferne unbenutzte Stops')
        # restrict_to_area keeps too many stops -> manually removing them
        # if not in stop times
        timetable = clip.get_stop_times()
        stop_ids_in_tt = timetable['stop_id'].unique()
        stops = clip.get_stops()
        is_in_tt = stops['stop_id'].isin(stop_ids_in_tt)
        clipped_stops = stops[is_in_tt]

        self.logger.info('Füge fehlende Elternstops aus dem Originalfeed hinzu')
        # keep the parent stations as well
        parent_ids = clipped_stops['parent_station']
        are_parents = stops['stop_id'].isin(parent_ids)
        stops['is_parent'] = are_parents
        stops = stops[is_in_tt | are_parents]
        clip.stops = stops

        self.simplify_route_types(clip)

        if self.do_visum_postproc:
            self.postprocess(clip)
            self.complement_parent_stations(clip)
            self.rename_stops(clip)

        if self.do_transferprocessing:
            self.complement_transfers(clip)

        self.logger.info(f'Schreibe verarbeiteten Feed nach {self.gtfs_output}')
        clip.write(self.gtfs_output)

    def postprocess(self, clip):
        stops = clip.get_stops()
        stops['idx'] = stops.index

        self.logger.info('Finde Routentypen und spalte Stops nach '
                         'Routentypen auf')
        tr = clip.get_trips().merge(clip.routes, how='left', on='route_id')
        tt_with_rt = clip.get_stop_times().merge(tr[['trip_id', 'route_type']],
                                                 how='left', on='trip_id')
        # stops with all route types stopping there
        st = tt_with_rt[['stop_id', 'route_type']].drop_duplicates()
        typed_stops = stops.merge(st, how='left', on='stop_id')

        self.logger.info('Führe aneinander liegende Stationen mit gleichem '
                         'Namen und Routentypen zusammen '
                         '(ausgenommen aufeinanderfolgende Stops) ')

        # new ids with trailing route type or old one if no routes are served
        no_route = typed_stops['route_type'].isna()
        typed_stops['route_int'] = typed_stops['route_type']
        typed_stops.loc[no_route, 'route_int'] = -1
        typed_stops['route_int'] = typed_stops['route_int'].astype(int)
        new_ids = (typed_stops['stop_id'].astype('str') + '_t' +
                   typed_stops['route_int'].astype('str'))
        new_ids.loc[no_route] = typed_stops.loc[no_route, 'stop_id']
        typed_stops['type_stop_id'] = new_ids

        gdf_stops = gp.GeoDataFrame(
            typed_stops, geometry=gp.points_from_xy(
                typed_stops['stop_lon'], typed_stops['stop_lat']),
            crs="EPSG:4326")
        gdf_stops.to_crs(3857, inplace=True)
        gdf_stops['stop_lon'] = gdf_stops['geometry'].apply(lambda geom: geom.x)
        gdf_stops['stop_lat'] = gdf_stops['geometry'].apply(lambda geom: geom.y)

        # find stations roughly at same location with same name and serving
        # same route type by putting lat/lon in seperate classes formed by 50m
        # (chained) differences
        gdf_stops = gdf_stops.sort_values('stop_lat')
        gdf_stops['lat_cl'] = gdf_stops[
            'stop_lat'].diff().fillna(0).abs().gt(50).cumsum()
        gdf_stops = gdf_stops.sort_values('stop_lon')
        gdf_stops['lon_cl'] = gdf_stops[
            'stop_lon'].diff().fillna(0).abs().gt(50).cumsum()

        # restore old order
        gdf_stops = gdf_stops.sort_values('idx').drop(columns=['idx'])

        subset = ['stop_name', 'route_type', 'lat_cl', 'lon_cl', 'level_id']
        duplicated = gdf_stops[gdf_stops.duplicated(
            subset=subset, keep='first')]

        # merge stops without the duplicated ones (= with first rows of
        # duplication appearance) to get relating stop ids of the rows that will
        # remain
        duplicated = duplicated.merge(
            gdf_stops[~gdf_stops['stop_id'].isin(duplicated['stop_id'])],
            how='left', on=subset, suffixes=['', '_remain'])

        ## exclude stops that are too distant to each other (the lat/lon above
        ## classification is not accurate enough in high density areas)
        #duplicated['distance'] = duplicated.apply(
            #lambda row: row['geometry'].distance(row['geometry_remain']),
            #axis=1)
        #duplicated = duplicated[duplicated['distance'] < 50]

        # exclude stops from removal that are adjacent in trips
        tt = tt_with_rt.merge(stops[['stop_id', 'stop_name']], how='left',
                              on='stop_id')
        # should be sorted already but better be safe
        tt = tt.sort_values(['trip_id', 'stop_sequence'])

        tt['next_stop_name'] = tt['stop_name'].shift(-1)
        tt['next_trip_id'] = tt['trip_id'].shift(-1)
        tt['next_stop_id'] = tt['stop_id'].shift(-1)
        # get stops in timetable where next stop name and trip is the same
        # but id is different
        first_dup_idx = ((tt['stop_name'] == tt['next_stop_name']) &
                         (tt['trip_id'] == tt['next_trip_id']) &
                         ~(tt['stop_id'] == tt['next_stop_id']))
        # get the next stops as well
        second_dup_idx = first_dup_idx.shift(1).fillna(False)
        chained_dup = tt[second_dup_idx | first_dup_idx]
        # exclude those stops from merging with other stops
        # paying attention to route type
        exclude = chained_dup.groupby(
            ['stop_id','route_type']).size().reset_index()[
                ['stop_id','route_type']]

        ex_idx = [False] * len(duplicated)
        # that seems a little excessive, no idea how to do it without a loop
        # it isn't a thing that happens a lot though
        # alternatively we could build and use type_stop_id
        for i, ex in exclude.iterrows():
            ex_idx = ex_idx | duplicated[
                ['stop_id', 'route_type']].isin(
                    (ex.stop_id, ex.route_type)).all(axis=1)
        stops_to_remove = duplicated[~ex_idx]

        # type stop id already contains route information so can be used
        # to remove unwanted stops by route type
        remove_ids = stops_to_remove['type_stop_id']
        # remove duplicated stops
        revised_stops = typed_stops.drop(
            typed_stops[typed_stops['type_stop_id'].isin(remove_ids)].index)

        self.logger.info('Ersetze IDs')

        # replace removed ids in timetable and stops with remaining ones
        # handle this per route type because some stops that are flagged
        # for removal might already have been split by route type
        reassign_map_union = {}
        for route_type in stops_to_remove['route_type'].unique():
            if route_type is None or gp.np.isnan(route_type):
                r_idx = stops_to_remove['route_type'].isna()
            else:
                r_idx = stops_to_remove['route_type'] == route_type
            route_stops = stops_to_remove[r_idx]
            reassign_map = dict(zip(route_stops['stop_id'],
                                    route_stops['stop_id_remain']))
            reassign_map_union.update(reassign_map)
            # route type None should only apply to parents, stops times without
            # route type are invalid
            if route_type is None or gp.np.isnan(route_type):
                continue
            loc_idx = tt_with_rt['route_type'] == route_type
            revised_ids = tt_with_rt.loc[loc_idx]['stop_id'].map(reassign_map)
            tt_with_rt.loc[loc_idx, 'stop_id_revised'] = revised_ids

        # same with parent ids in stops
        revised_stops['parent_id_revised'] = revised_stops[
            'parent_station'].map(reassign_map_union)
        revised_stops.loc[revised_stops['parent_id_revised'].isna(),
                          'parent_id_revised'] = revised_stops['parent_station']
        revised_stops = revised_stops.merge(
            # there should be no parents with route types so no duplicates
            # for them but better be safe to avoid duplicating rows
            revised_stops[['stop_id', 'type_stop_id']].drop_duplicates(),
            how='left', left_on='parent_id_revised', right_on='stop_id',
            suffixes=['', '_1'])
        # replace old ids with new ones
        revised_stops.rename(
            columns={'parent_id_revised': 'original_parent_station'},
            inplace=True)
        revised_stops['parent_station'] = revised_stops['type_stop_id_1']

        # transfers.txt has stop ids that need to be replaced as well

        # ToDo: 200m transfers, wenn nicht vorhanden, 3kmh Gehgeschw., + 2 min
        # IDs
        revised_transfers = clip.transfers.copy()
        for column in ['from_stop_id', 'to_stop_id']:
            revised_transfers['stop_id_revised'] = revised_transfers.loc[
                :, column].map(reassign_map_union)
            revised_transfers.loc[revised_transfers['stop_id_revised'].isna(),
                              'stop_id_revised'] = revised_transfers.loc[
                                  :, column]
            revised_transfers = revised_transfers.merge(
                # there should be no parents with route types so no duplicates
                # for them but better be safe to avoid duplicating rows
                revised_stops[['stop_id', 'type_stop_id']].drop_duplicates(),
                how='left', left_on='stop_id_revised', right_on='stop_id')
            revised_transfers.loc[:, column] = revised_transfers.loc[
                :, 'type_stop_id']
            revised_transfers.drop(
                columns=['stop_id_revised', 'stop_id', 'type_stop_id'],
                inplace=True)
        ## drop lines where from an to stop is identical after reassigning ids
        #redundant = revised_transfers['from_stop_id'] == revised_transfers[
            #'to_stop_id']
        #revised_transfers.drop(index=revised_transfers[redundant].index,
                               #inplace=True)

        # fill column with revised ids with the original id in case they
        # are not reassigned
        tt_with_rt.loc[tt_with_rt['stop_id_revised'].isna(),
                      'stop_id_revised'] = tt_with_rt['stop_id']
        tt_with_rt['stop_id'] = tt_with_rt['stop_id_revised']
        # adding new stop ids to timetable
        tt_revised = tt_with_rt.merge(
            revised_stops[['stop_id', 'type_stop_id',
                           'route_type']].drop_duplicates(),
            how='left', on=['stop_id', 'route_type'])

        revised_stops['stop_id'] = revised_stops['type_stop_id']
        tt_revised['stop_id'] = tt_revised['type_stop_id']
        tt_revised = tt_revised.sort_values(['trip_id', 'stop_sequence'])

        # ToDo: remove unused parent stations

        self.logger.info('Versetze Stops mit gleicher ID und '
                         'gleichen Koordinaten')
        # stops with same original id indicate that they are split and at same
        # coordinates -> scatter them slightly
        duplicated = revised_stops[revised_stops.duplicated(
            subset=['stop_id'], keep=False)]
        grp = duplicated.groupby('stop_id')
        duplicated['grp_idx'] = grp.cumcount()
        duplicated = duplicated.reset_index().merge(
            grp.size().reset_index(name='grp_count'),
            on='stop_id', how='left').set_index('index')
        shift_x_y = 0.0001
        duplicated['shift_angle'] = ((2 * math.pi) / duplicated['grp_count'] *
                                     duplicated['grp_idx'])
        duplicated['stop_lat_shifted'] = (
            duplicated['stop_lat'] +
            duplicated['shift_angle'].apply(math.sin) * shift_x_y)
        duplicated['stop_lon_shifted'] = (
            duplicated['stop_lon'] +
            duplicated['shift_angle'].apply(math.cos) * shift_x_y)

        revised_stops.loc[duplicated.index,
                          'stop_lat'] = duplicated['stop_lat_shifted']
        revised_stops.loc[duplicated.index,
                          'stop_lon'] = duplicated['stop_lon_shifted']

        # set all appearances of locations with types other than 0 (platform)
        # or 1 (station) (meaning entrance or exit to station)
        # most likely caused by faulty input feed, as trips only hold at
        # location_type 0 and we only keep those and the parents (should always
        # be 1)
        revised_stops.loc[revised_stops['location_type'] > 1,
                          'location_type'] = 0

        clip.stops = revised_stops.drop(
            columns=['route_int', 'stop_id_1', 'type_stop_id_1',
                     'type_stop_id'])

        clip.stop_times = tt_revised.drop(
            columns=['stop_id_revised', 'route_type', 'type_stop_id'])

        clip.transfers = revised_transfers

    def complement_transfers(self, clip):
        self.logger.info('Berechne Distanzen und Transferzeiten zwischen '
                         'den Stops')
        stops_df = clip.get_stops()
        stops_df = stops_df[~stops_df['is_parent']]
        gdf_stops = gp.GeoDataFrame(stops_df, geometry=gp.points_from_xy(
            stops_df['stop_lon'], stops_df['stop_lat']), crs="EPSG:4326")
        gdf_stops.to_crs(3857, inplace=True)
        gdf_stops.index = gdf_stops['stop_id']
        dist_matrix = gdf_stops.geometry.apply(lambda g: gdf_stops.distance(g))
        dist_matrix.index = gdf_stops['stop_id']
        dist_df = dist_matrix.stack().to_frame().rename(columns={0: 'distance'})
        dist_df.index.rename(['from_stop_id', 'to_stop_id'], inplace=True)
        dist_df.reset_index(inplace=True)
        dist_df = dist_df[dist_df['distance'] <= TRANSFER_MAX_DISTANCE]
        dist_df['min_transfer_time'] = dist_df['distance'].apply(
            lambda x: ADD_TRANSFER_TIME * 60 + x * 3.6 / TRANSFER_SPEED * 1000)
        dist_df.drop(columns=['distance'], inplace=True)
        # type 2 - "Transfer requires a minimum amount of time between arrival
        # and departure to ensure a connection"
        dist_df['transfer_type'] = 2
        transfers_df = clip.transfers.copy()
        #
        ex_transfers = pd.concat([transfers_df, dist_df], ignore_index=True)
        # remove transfers that were already in by keeping the first occurence
        # (first ones are from the "original" transfers because concat)
        ex_transfers.drop_duplicates(subset=['from_stop_id', 'to_stop_id'],
                                     keep='first', inplace=True)
        n = len(ex_transfers) - len(transfers_df)
        self.logger.info(f'{n} Transfers hinzugefügt (max Distanz '
                         f'{TRANSFER_MAX_DISTANCE} mit {TRANSFER_SPEED}km/h '
                         f'und {ADD_TRANSFER_TIME}min Aufschlag)')
        clip.transfers = ex_transfers

    def simplify_route_types(self, clip):
        routes_df = clip.get_routes()
        revised_types = routes_df['route_type'].map(ROUTE_TYPE_MAP)
        nan_values = revised_types.isna()
        revised_types.loc[nan_values] = routes_df.loc[nan_values, 'route_type']
        routes_df['route_type'] = revised_types.astype(int)
        clip.routes = routes_df

    def complement_parent_stations(self, clip):
        self.logger.info('Erzeuge Elternstationen für Stops, wenn sie '
                         'beieinander liegen und noch keine haben')
        stops_df = clip.get_stops()
        stops_df_nop = stops_df[~stops_df['is_parent'] &
                                stops_df['parent_station'].isna()]
        stop_times_df = clip.get_stop_times()
        dep_df = stops_df_nop.merge(stop_times_df, how='inner', on='stop_id')
        dep_count = dep_df.groupby('stop_id').size().reset_index().rename(
            columns={0: 'n_departures'})
        stops_df_nop = stops_df_nop.merge(dep_count, how='left', on='stop_id')
        gdf_stops = gp.GeoDataFrame(stops_df_nop, geometry=gp.points_from_xy(
            stops_df_nop['stop_lon'], stops_df_nop['stop_lat']),
                                    crs="EPSG:4326")
        gdf_stops.to_crs(3857, inplace=True)
        gdf_stops['stop_lon'] = gdf_stops['geometry'].apply(lambda geom: geom.x)
        gdf_stops['stop_lat'] = gdf_stops['geometry'].apply(lambda geom: geom.y)

        # find stations roughly at same location by chained distance grouping
        # in 150m steps along lon and lat
        gdf_stops = gdf_stops.sort_values('stop_lat')
        gdf_stops['lat_cl'] = gdf_stops[
            'stop_lat'].diff().fillna(0).abs().gt(150).cumsum()
        gdf_stops = gdf_stops.sort_values('stop_lon')
        gdf_stops['lon_cl'] = gdf_stops[
            'stop_lon'].diff().fillna(0).abs().gt(150).cumsum()
        idx = stops_df.index.max() + 1
        for i, group in gdf_stops.groupby(['lat_cl', 'lon_cl']):
            if len(group) < 2:
                continue
            wo_dep = group[group['n_departures'].isna()]
            # if there is a station in the group without take it as new
            # parent station
            if len(wo_dep) > 0:
                station_id = wo_dep.loc[group.index[0]].stop_id
                child_ids = group[~(group['stop_id'] == station_id)]['stop_id']
            else:
                centroid = group.dissolve().centroid.to_crs(4326)
                station_template = group.loc[group['n_departures'].idxmax()]
                station_id = station_template.stop_id.split('_')[0] + f'_parent'
                new_row = pd.Series({
                    'stop_id': station_id,
                    'stop_name': station_template.stop_name,
                    'stop_lon': centroid.x.values[0],
                    'stop_lat': centroid.y.values[0],
                    'location_type': 1,
                    'is_parent': True})
                stops_df.loc[idx] = new_row
                child_ids = group['stop_id']
            stops_df.loc[
                stops_df[stops_df['stop_id'].isin(child_ids)].index,
                'parent_station'] = station_id
            idx += 1
        self.logger.info(f'{len(stops_df) - len(clip.get_stops())} Stationen '
                         'hinzugefügt')
        clip.stops = stops_df
        # wenn in Gruppe eine Station ohne Abfahrten, dann die als parent
        # als name für neue parents die station mit den meisten Abfahrten
        # generell: Station mit parent -> stop_desc in Klammern an stop_name

    def rename_stops(self, clip):
        self.logger.info('Füge Stationsbeschreibungen zu Stationsnamen hinzu')
        stops_df = clip.get_stops()
        stops_w_desc = stops_df[~stops_df['stop_desc'].isna()]
        new_names = stops_w_desc['stop_name'] + ' (' + \
            stops_w_desc['stop_desc'] + ')'
        stops_df.loc[stops_w_desc.index, 'stop_name'] = new_names
        self.logger.info(f'{len(stops_w_desc)} Stationen umbenannt')
        clip.stops = stops_df
