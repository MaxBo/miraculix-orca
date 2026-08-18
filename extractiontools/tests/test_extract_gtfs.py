"""Tests for GTFS transformations that do not require external data."""

import unittest

import numpy as np
import pandas as pd

from extractiontools.extract_gtfs import ExtractGTFS


class InMemoryFeed:
    """Small subset of the gtfs-kit Feed interface used by ExtractGTFS."""

    def __init__(
        self,
        *,
        stops=None,
        routes=None,
        trips=None,
        stop_times=None,
        transfers=None,
    ):
        self.stops = stops
        self.routes = routes
        self.trips = trips
        self.stop_times = stop_times
        self.transfers = transfers

    def get_stops(self):
        return self.stops

    def get_routes(self):
        return self.routes

    def get_trips(self):
        return self.trips

    def get_stop_times(self):
        return self.stop_times


class TestExtractGTFS(unittest.TestCase):
    """Exercise the in-memory transformations performed by ExtractGTFS."""

    def setUp(self):
        self.extractor = ExtractGTFS(
            project_area=None,
            gtfs_input="unused.zip",
            out_path="unused",
        )

    def test_simplify_route_types_maps_extended_types(self):
        feed = InMemoryFeed(
            routes=pd.DataFrame(
                {
                    "route_id": ["a", "b", "c", "d", "e", "f"],
                    "route_type": [100, 400, 700, 1702, 3, 999],
                }
            )
        )

        self.extractor.simplify_route_types(feed)

        self.assertListEqual(feed.routes["route_type"].tolist(), [2, 0, 3, 0, 3, 999])
        self.assertTrue(pd.api.types.is_integer_dtype(feed.routes["route_type"]))

    def test_rename_stops_appends_non_null_descriptions(self):
        feed = InMemoryFeed(
            stops=pd.DataFrame(
                {
                    "stop_id": ["a", "b"],
                    "stop_name": ["Central", "West"],
                    "stop_desc": ["platform 1", np.nan],
                }
            )
        )

        self.extractor.rename_stops(feed)

        names = feed.stops.set_index("stop_id")["stop_name"].to_dict()
        self.assertEqual(names, {"a": "Central (platform 1)", "b": "West"})

    def test_rename_stops_accepts_feeds_without_stop_desc(self):
        original = pd.DataFrame({"stop_id": ["a"], "stop_name": ["Central"]})
        feed = InMemoryFeed(stops=original.copy())

        self.extractor.rename_stops(feed)

        pd.testing.assert_frame_equal(feed.stops, original)

    def test_complement_transfers_uses_only_non_parent_stops(self):
        feed = InMemoryFeed(
            stops=pd.DataFrame(
                {
                    "stop_id": ["a", "b", "parent"],
                    "stop_lon": [8.0, 8.0, 8.0],
                    "stop_lat": [50.0, 50.0, 50.0],
                    "is_parent": [False, False, True],
                }
            ),
            transfers=pd.DataFrame(
                {
                    "from_stop_id": ["a"],
                    "to_stop_id": ["b"],
                    "transfer_type": [0],
                    "min_transfer_time": [0],
                }
            ),
        )

        self.extractor.complement_transfers(feed)

        pairs = set(zip(feed.transfers["from_stop_id"], feed.transfers["to_stop_id"]))
        self.assertSetEqual(pairs, {("a", "a"), ("a", "b"), ("b", "a"), ("b", "b")})
        self.assertNotIn("parent", set(feed.transfers["from_stop_id"]))
        self.assertNotIn("parent", set(feed.transfers["to_stop_id"]))

        existing = feed.transfers[
            (feed.transfers["from_stop_id"] == "a")
            & (feed.transfers["to_stop_id"] == "b")
        ]
        self.assertEqual(len(existing), 1)
        self.assertEqual(existing.iloc[0]["transfer_type"], 0)

        generated = feed.transfers[
            (feed.transfers["from_stop_id"] == "b")
            & (feed.transfers["to_stop_id"] == "a")
        ].iloc[0]
        self.assertEqual(generated["transfer_type"], 2)
        self.assertEqual(generated["min_transfer_time"], 120)

    def test_complement_parent_stations_creates_parent_for_nearby_stops(self):
        feed = InMemoryFeed(
            stops=pd.DataFrame(
                {
                    "stop_id": ["central_bus", "central_tram"],
                    "stop_name": ["Central", "Central"],
                    "stop_lon": [8.0, 8.0],
                    "stop_lat": [50.0, 50.0],
                    "location_type": [0, 0],
                    "is_parent": [False, False],
                    "parent_station": pd.Series([None, None], dtype="object"),
                }
            ),
            stop_times=pd.DataFrame(
                {
                    "trip_id": ["bus-1", "bus-2", "tram-1"],
                    "stop_id": ["central_bus", "central_bus", "central_tram"],
                    "stop_sequence": [1, 1, 1],
                }
            ),
        )

        self.extractor.complement_parent_stations(feed)

        parents = feed.stops[feed.stops["is_parent"]]
        self.assertEqual(len(parents), 1)
        parent = parents.iloc[0]
        self.assertEqual(parent["stop_id"], "central_parent")
        self.assertEqual(parent["stop_name"], "Central")
        self.assertEqual(parent["location_type"], 1)

        children = feed.stops[feed.stops["stop_id"].isin(["central_bus", "central_tram"])]
        self.assertSetEqual(set(children["parent_station"]), {"central_parent"})

    def test_postprocess_splits_a_stop_by_route_type(self):
        feed = InMemoryFeed(
            stops=pd.DataFrame(
                {
                    "stop_id": ["central"],
                    "stop_name": ["Central"],
                    "stop_lon": [8.0],
                    "stop_lat": [50.0],
                    "location_type": [0],
                    "parent_station": pd.Series([None], dtype="object"),
                    "is_parent": [False],
                }
            ),
            routes=pd.DataFrame(
                {
                    "route_id": ["rail", "bus"],
                    "route_type": [2, 3],
                }
            ),
            trips=pd.DataFrame(
                {
                    "route_id": ["rail", "bus"],
                    "service_id": ["daily", "daily"],
                    "trip_id": ["rail-trip", "bus-trip"],
                }
            ),
            stop_times=pd.DataFrame(
                {
                    "trip_id": ["rail-trip", "bus-trip"],
                    "arrival_time": ["08:00:00", "08:05:00"],
                    "departure_time": ["08:00:00", "08:05:00"],
                    "stop_id": ["central", "central"],
                    "stop_sequence": [1, 1],
                }
            ),
            transfers=None,
        )

        self.extractor.postprocess(feed)

        self.assertSetEqual(set(feed.stops["stop_id"]), {"central_t2", "central_t3"})
        stop_ids_by_trip = feed.stop_times.set_index("trip_id")["stop_id"].to_dict()
        self.assertEqual(
            stop_ids_by_trip,
            {"rail-trip": "central_t2", "bus-trip": "central_t3"},
        )
        self.assertTrue((feed.stops["location_type"] == 0).all())


if __name__ == "__main__":
    unittest.main()
