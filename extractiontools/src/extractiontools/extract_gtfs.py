import gtfs_kit as gk
import geopandas as gp
import logging
import os
import math

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


class ExtractGTFS():

    def __init__(self,
                 project_area: 'ogr.Geometry',
                 gtfs_input: str,
                 out_path: str,
                 logger=None):
        """"""
        self.gtfs_input = gtfs_input
        self.out_path = out_path
        #self.out_path = "D:\\Downloads"
        #self.gtfs_input = os.path.join(self.out_path, 'gtfsde_latest.zip')
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

        self.logger.info('Füge fehlende Elternstops hinzu')
        # keep the parent stations as well
        parent_ids = clipped_stops['parent_station']
        are_parents = stops['stop_id'].isin(parent_ids)
        stops['is_parent'] = are_parents
        stops = stops[is_in_tt | are_parents]

        self.logger.info('Finde Routentypen und spalte Stops nach '
                         'Routentypen auf')
        tr = clip.get_trips().merge(clip.routes, how='left', on='route_id')
        tt_with_rt = clip.get_stop_times().merge(tr[['trip_id', 'route_type']],
                                                 how='left', on='trip_id')
        # stops with all route types stopping there
        st = tt_with_rt[['stop_id', 'route_type']].drop_duplicates()
        st['route_type'] = st['route_type'].astype(int)
        typed_stops = stops.merge(st, how='left', on='stop_id')
        typed_stops['stop_int'] = typed_stops['stop_id'].astype(int)
        typed_stops = typed_stops.sort_values('stop_int')

        self.logger.info('Führe aneinander liegende Stationen mit gleichem '
                         'Namen und Routentypen zusammen '
                         '(ausgenommen aufeinanderfolgende Stops) ')
        # find stations roughly at same location with same name and serving
        # same route type by putting lat/lon in seperate classes formed by 0.01
        # differences
        typed_stops = typed_stops.sort_values('stop_lat')
        typed_stops['lat_cl'] = typed_stops[
            'stop_lat'].diff().fillna(0).abs().gt(0.01).cumsum().values
        typed_stops = typed_stops.sort_values('stop_lon')
        typed_stops['lon_cl'] = typed_stops[
            'stop_lon'].diff().fillna(0).abs().gt(0.01).cumsum().values

        typed_stops = typed_stops.sort_values('stop_int')
        subset = ['stop_name', 'route_type', 'lat_cl', 'lon_cl']
        duplicated = typed_stops[typed_stops.duplicated(
            subset=subset, keep='first')]
        # merge stops without the duplicated ones (= with first rows of
        # duplication appearance) to get relating stop ids of the rows that will
        # remain
        duplicated = duplicated.merge(
            typed_stops[~typed_stops['stop_id'].isin(duplicated['stop_id'])],
            how='left', on=subset, suffixes=['', '_remain'])
        tt = timetable.merge(stops[['stop_id', 'stop_name']], how='left',
                             on='stop_id')
        # should be sorted already but better be safe
        tt = tt.sort_values(['trip_id', 'stop_sequence'])
        # set first stop in every trip to None so when shifting we won't get
        # stop ids and names from next trip
        tt.loc[tt['stop_sequence'] == 0, ['stop_name', 'stop_id']] = None
        tt['next_stop_name'] = tt['stop_name'].shift(-1)
        tt['next_stop_id'] = tt['stop_id'].shift(-1)
        # get stops in timetable where next stop name is the same
        # but id is different
        chained_dup = tt[(tt['stop_name'] == tt['next_stop_name']) &
               ~(tt['stop_id'] == tt['next_stop_id'])]
        # exclude those stops from merging with other stops
        excluded_merge = chained_dup['stop_id'].unique()
        stops_to_remove = duplicated[
            ~(duplicated['stop_id'].isin(excluded_merge))]
        remove_ids = stops_to_remove['stop_id']
        # remove duplicated stops
        revised_stops = typed_stops.drop(
            typed_stops[typed_stops['stop_id'].isin(remove_ids)].index)

        self.logger.info('Vergebe neue IDs')
        # new ids with leading route type or old one if no routes are served
        new_ids = (revised_stops['stop_int'] +
                   (revised_stops['route_type'] * 1000000))
        no_route = revised_stops['route_type'].isna()
        new_ids.loc[no_route] = 0
        # convert to string and fill with leading zeros (in case route_type is 0)
        new_ids = new_ids.astype(int).astype(str).apply(lambda a: a.zfill(7))
        new_ids.loc[no_route] = revised_stops.loc[no_route, 'stop_id']
        revised_stops['type_stop_id'] = new_ids

        # replace removed ids in timetable and stops with remaining ones
        reassign_map = dict(zip(stops_to_remove['stop_id'],
                                stops_to_remove['stop_id_remain']))
        tt_with_rt['stop_id_revised'] = tt_with_rt['stop_id'].map(reassign_map)
        tt_with_rt.loc[tt_with_rt['stop_id_revised'].isna(),
                      'stop_id_revised'] = tt_with_rt['stop_id']
        tt_with_rt['stop_id'] = tt_with_rt['stop_id_revised']
        #for i, row in stops_to_remove.iterrows():
            #timetable.loc[timetable['stop_id'] == row.stop_id,
                          #'stop_id'] = row.stop_id_remain
        # adding new stop ids
        tt_revised = tt_with_rt.merge(
            revised_stops[['stop_id', 'type_stop_id', 'route_type']].drop_duplicates(),
            how='left', on=['stop_id', 'route_type'])
        tt_revised = tt_revised.sort_values(['trip_id', 'stop_sequence'])

        # same with parent ids in stops (don't know if it can even happen
        # that parents are duplicated)
        revised_stops['parent_id_revised'] = revised_stops[
            'parent_station'].map(reassign_map)
        revised_stops.loc[revised_stops['parent_id_revised'].isna(),
                          'parent_id_revised'] = revised_stops['parent_station']

        revised_stops['parent_station'] = revised_stops['parent_id_revised']
        self.logger.info('Versetze Stops mit gleicher ID und gleichen Koordinaten')
        # identify stops at same position with same id and scatter them slightly
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

        clip.stops = revised_stops.drop(
            columns=['stop_int', 'lat_cl', 'lon_cl', 'parent_id_revised'])
        clip.stop_times = tt_revised.drop(
            columns=['stop_id_revised', 'route_type'])

        self.logger.info(f'Schreibe verarbeiteten Feed nach {self.gtfs_output}')
        clip.write(self.gtfs_output)