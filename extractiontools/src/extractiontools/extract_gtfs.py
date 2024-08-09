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
        typed_stops = stops.merge(st, how='left', on='stop_id')
        typed_stops['stop_int'] = typed_stops['stop_id'].astype(int)

        self.logger.info('Führe aneinander liegende Stationen mit gleichem '
                         'Namen und Routentypen zusammen '
                         '(ausgenommen aufeinanderfolgende Stops) ')
        # find stations roughly at same location with same name and serving
        # same route type by putting lat/lon in seperate classes formed by 0.01
        # differences
        typed_stops = typed_stops.sort_values('stop_lat')
        typed_stops['lat_cl'] = typed_stops[
            'stop_lat'].diff().fillna(0).abs().gt(0.003).cumsum()
        typed_stops = typed_stops.sort_values('stop_lon')
        typed_stops['lon_cl'] = typed_stops[
            'stop_lon'].diff().fillna(0).abs().gt(0.003).cumsum()

        typed_stops = typed_stops.sort_values('stop_int')

        # new ids with leading route type or old one if no routes are served
        new_ids = (typed_stops['stop_int'] +
                   (typed_stops['route_type'] * 1000000))
        no_route = typed_stops['route_type'].isna()
        new_ids.loc[no_route] = 0
        # convert to string and fill with leading zeros (in case route_type is 0)
        new_ids = new_ids.astype(int).astype(str).apply(lambda a: a.zfill(7))
        new_ids.loc[no_route] = typed_stops.loc[no_route, 'stop_id']
        typed_stops['type_stop_id'] = new_ids

        gdf_stops = gp.GeoDataFrame(
            typed_stops, geometry=gp.points_from_xy(
                typed_stops['stop_lon'], typed_stops['stop_lat']),
            crs="EPSG:4326")
        subset = ['stop_name', 'route_type', 'lat_cl', 'lon_cl']
        duplicated = gdf_stops[gdf_stops.duplicated(
            subset=subset, keep='first')]

        # merge stops without the duplicated ones (= with first rows of
        # duplication appearance) to get relating stop ids of the rows that will
        # remain
        duplicated = duplicated.merge(
            gdf_stops[~gdf_stops['stop_id'].isin(duplicated['stop_id'])],
            how='left', on=subset, suffixes=['', '_remain'])

        # exclude stops that are too distant to each other (the lat/lon above
        # classification is not accurate enough in high density areas)
        duplicated['distance'] = duplicated.apply(
            lambda row: row['geometry'].distance(row['geometry_remain']),
            axis=1)
        duplicated = duplicated[duplicated['distance'] < 0.003]

        # exclude stops from removal that are adjacent in trips
        tt = tt_with_rt.merge(stops[['stop_id', 'stop_name']], how='left',
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
        # paying attention to route type
        exclude = chained_dup.groupby(
            ['stop_id','route_type']).size().reset_index()[['stop_id','route_type']]
        ex_idx = [False] * len(duplicated)
        # that seems a little excessive, no idea how to do it without a loop
        # it isn't a thing that happens a lot though
        # alternatively we could build and use type_stop_id
        for i, ex in exclude.iterrows():
            ex_idx = ex_idx | duplicated[
                ['stop_id', 'route_type']].isin(
                    (ex.stop_id, ex.route_type)).all(axis=1)
        stops_to_remove = duplicated[~ex_idx]

        ## same problem as loop above
        #rem_idx = [False] * len(typed_stops)
        #for i, rem in stops_to_remove.iterrows():
            #rem_idx = rem_idx | typed_stops[
                #['stop_id', 'route_type']].isin(
                    #(rem.stop_id, rem.route_type)).all(axis=1)

        # type stop id already contains route information so can be used
        # to remove unwanted stops by route type
        remove_ids = stops_to_remove['type_stop_id']
        # remove duplicated stops
        revised_stops = typed_stops.drop(
            typed_stops[typed_stops['type_stop_id'].isin(remove_ids)].index)

        self.logger.info('Ersetze IDs')

        #351337 der ist auseinandergenommen worden, dann aber geflaggt zum entfernen (bleibend 298537) -> für routetype 3 fehlt er dann
        #      stop_name  stop_lat  stop_lon  route_type
        #9231  Kreiensen  51.85212  9.967361         2.0
        #9233  Kreiensen  51.85182  9.965966         2.0
        #9234  Kreiensen  51.85182  9.965966         3.0

        # komische Struktur für 'Bernterode, Abzweig Krombach':
        #     stop_id parent_station   stop_lat   stop_lon
        #2285  114940            NaN  51.300297  10.156758
        #2286  125225            NaN  51.323742  10.057641
        #2288  149968         125225  51.323742  10.057641
        #2287  522494         114940  51.300297  10.156758

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

        # ToDo: transfers.txt has stop ids that need to be replaced as well

        # replace old ids with new ones
        revised_stops.rename(
            columns={'parent_id_revised': 'original_parent_station'},
            inplace=True)
        revised_stops['parent_station'] = revised_stops['type_stop_id_1']

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

        revised_stops.rename(columns={'stop_id': 'original_stop_id'}, inplace=True)
        revised_stops.rename(columns={'type_stop_id': 'stop_id'}, inplace=True)
        tt_revised.rename(columns={'stop_id': 'original_stop_id'}, inplace=True)
        tt_revised.rename(columns={'type_stop_id': 'stop_id'}, inplace=True)
        tt_revised = tt_revised.sort_values(['trip_id', 'stop_sequence'])

        self.logger.info('Versetze Stops mit gleicher ID und '
                         'gleichen Koordinaten')
        # stops with same original id indicate that they are split and at same
        # coordinates -> scatter them slightly
        duplicated = revised_stops[revised_stops.duplicated(
            subset=['original_stop_id'], keep=False)]
        grp = duplicated.groupby('original_stop_id')
        duplicated['grp_idx'] = grp.cumcount()
        duplicated = duplicated.reset_index().merge(
            grp.size().reset_index(name='grp_count'),
            on='original_stop_id', how='left').set_index('index')
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
            columns=['stop_int', 'lat_cl', 'lon_cl', 'stop_id_1',
                     'type_stop_id_1'])

        clip.stop_times = tt_revised.drop(
            columns=['stop_id_revised', 'route_type'])

        self.logger.info(f'Schreibe verarbeiteten Feed nach {self.gtfs_output}')
        clip.write(self.gtfs_output)