import gtfs_kit as gk
import geopandas as gp
import logging
import os


class ExtractGTFS():

    def __init__(self,
                 project_area: 'ogr.Geometry',
                 gtfs_input: str,
                 out_path: str,
                 logger=None):
        """"""
        self.gtfs_input = gtfs_input
        self.out_path = out_path
        self.gtfs_output = os.path.join(self.out_path, 'gtfs_clipped.zip')
        self.project_area = project_area
        self.logger = logger or logging.getLogger(self.__module__)

    def extract(self):
        wkt = self.project_area.ExportToWkt()
        self.logger.info(f'Lade Feed aus der GTFS-Datei {self.gtfs_input}')
        area = gp.GeoDataFrame(geometry=gp.GeoSeries.from_wkt([wkt], crs=4326))
        feed = gk.read_feed(self.gtfs_input, dist_units='km')
        clip = gk.miscellany.restrict_to_area(feed, area)
        del(feed)
        # restrict_to_area keeps too many stops -> manually removing them
        # if not in stop times
        timetable = clip.get_stop_times()
        stop_ids_in_tt = timetable['stop_id'].unique()
        stops = clip.get_stops()
        is_in_tt = stops['stop_id'].isin(stop_ids_in_tt)
        clipped_stops = stops[is_in_tt]
        # keep the parent stations as well
        parent_ids = clipped_stops['parent_station']
        are_parents = stops['stop_id'].isin(parent_ids)
        clip.stops = stops[is_in_tt | are_parents]
        self.logger.info(f'Schreibe beschnittenen Feed nach {self.gtfs_output}')
        clip.write(self.gtfs_output)