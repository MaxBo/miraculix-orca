"""Tests for the shared Extract lifecycle and database-free subclass hooks."""

import datetime
import unittest
from unittest import mock

from extractiontools.ausschnitt import Extract
from extractiontools.extract_landuse import ExtractLanduse
from extractiontools.extract_osm import ExtractOSM
from extractiontools.extract_verwaltungsgrenzen import ExtractVerwaltungsgrenzen
from extractiontools.scrape_stops import ScrapeStops
from extractiontools.scrape_timetable import ScrapeTimetable


def connection_context(connection):
    context = mock.MagicMock()
    context.__enter__.return_value = connection
    return context


class TestExtractLifecycle(unittest.TestCase):
    def test_extract_runs_shared_lifecycle_and_subclass_hooks(self):
        boundary = object()
        extract = Extract(
            destination_db="project",
            target_srid=25832,
            boundary=boundary,
            tables={"nodes": "geom"},
        )
        primary_connection = mock.MagicMock()
        cleanup_connection = mock.MagicMock()
        for method in (
            "set_pg_path", "set_target_boundary", "update_boundaries", "create_schema",
            "create_foreign_catalog", "create_foreign_schema", "extract_table",
            "additional_stuff", "final_stuff", "further_stuff", "cleanup",
        ):
            setattr(extract, method, mock.Mock())

        with mock.patch(
            "extractiontools.ausschnitt.Connection",
            side_effect=[connection_context(primary_connection), connection_context(cleanup_connection)],
        ) as connection_class:
            extract.extract()

        self.assertEqual(connection_class.call_count, 2)
        extract.set_pg_path.assert_called_once_with()
        extract.set_target_boundary.assert_called_once_with(boundary, name="bbox")
        extract.update_boundaries.assert_called_once_with()
        extract.create_schema.assert_called_once_with(None, conn=primary_connection, replace=True)
        extract.create_foreign_catalog.assert_called_once_with()
        extract.create_foreign_schema.assert_called_once_with()
        extract.extract_table.assert_called_once_with("nodes", geom="geom")
        extract.additional_stuff.assert_called_once_with()
        extract.final_stuff.assert_called_once_with()
        extract.further_stuff.assert_called_once_with()
        self.assertEqual(
            extract.cleanup.call_args_list,
            [
                mock.call(conn=cleanup_connection),
                mock.call(conn=cleanup_connection, schema="temp_pg_catalog"),
            ],
        )

    def test_simple_subclass_hooks_delegate_to_shared_helpers(self):
        landuse = object.__new__(ExtractLanduse)
        self.assertEqual(landuse.get_corine_raster_name("clc18"), "clc18_raster")

        boundaries = object.__new__(ExtractVerwaltungsgrenzen)
        boundaries.schema = "verwaltungsgrenzen"
        boundaries.tables = {"gem": "geom", "krs": "geom"}
        boundaries.copy_constraints_and_indices = mock.Mock()
        boundaries.final_stuff()
        boundaries.copy_constraints_and_indices.assert_called_once_with(
            "verwaltungsgrenzen", ["gem", "krs"]
        )

    def test_osm_additional_stuff_executes_its_extension_hooks(self):
        extractor = object.__new__(ExtractOSM)
        extractor.conn = mock.MagicMock()
        for method in (
            "set_session", "extract_nodes", "extract_ways", "copy_way_nodes", "copy_relations",
            "copy_users", "copy_schema_info", "add_comments", "remove_session",
        ):
            setattr(extractor, method, mock.Mock())

        extractor.additional_stuff()

        for method in (
            "set_session", "extract_nodes", "extract_ways", "copy_way_nodes", "copy_relations",
            "copy_users", "copy_schema_info", "add_comments", "remove_session",
        ):
            getattr(extractor, method).assert_called_once_with()
        self.assertEqual(extractor.conn.commit.call_count, 1)


class TestScrapeStops(unittest.TestCase):
    def setUp(self):
        self.scraper = ScrapeStops(destination_db="project", target_srid=25832)
        self.scraper.schema = "timetables"
        self.scraper.boundary_name = "project_area"

    def test_scrape_runs_schema_stop_and_route_steps(self):
        connection = mock.MagicMock()
        self.scraper.create_schema = mock.Mock()
        self.scraper.scrape_haltestellen = mock.Mock()
        self.scraper.copy_route_types = mock.Mock()

        with mock.patch(
            "extractiontools.scrape_stops.Connection",
            return_value=connection_context(connection),
        ):
            self.scraper.scrape()

        self.scraper.create_schema.assert_called_once_with("timetables", conn=connection, replace=False)
        self.scraper.scrape_haltestellen.assert_called_once_with()
        self.scraper.copy_route_types.assert_called_once_with()
        self.assertEqual(connection.commit.call_count, 2)

    def test_get_stops_at_point_inserts_returned_stops_and_commits(self):
        cursor = mock.MagicMock()
        cursor.rowcount = 2
        self.scraper.conn = mock.MagicMock()
        self.scraper.run_query = mock.Mock()
        db_query = mock.Mock()
        db_query.stops_near.return_value = [
            {"name": "Central", "id": 1, "x": 9.1, "y": 54.1},
            {"name": "West", "id": 2, "x": 9.2, "y": 54.2},
        ]

        with mock.patch("extractiontools.scrape_stops.time.sleep"):
            count = self.scraper.get_stops_at_point(
                (9.0, 54.0), 1_000, db_query, cursor, "temporary_stops"
            )

        self.assertEqual(count, 2)
        db_query.stops_near.assert_called_once_with((9.0, 54.0), max_distance=1_000)
        self.assertIn("temporary_stops", self.scraper.run_query.call_args.args[0])
        self.assertEqual(cursor.execute.call_count, 4)
        self.scraper.conn.commit.assert_called_once_with()

    def test_get_stops_at_point_returns_zero_without_database_writes_for_empty_response(self):
        self.scraper.run_query = mock.Mock()
        db_query = mock.Mock()
        db_query.stops_near.return_value = []

        with mock.patch("extractiontools.scrape_stops.time.sleep"):
            count = self.scraper.get_stops_at_point(
                (9.0, 54.0), 1_000, db_query, mock.Mock(), "temporary_stops"
            )

        self.assertEqual(count, 0)
        self.scraper.run_query.assert_not_called()


class TestScrapeTimetable(unittest.TestCase):
    def setUp(self):
        self.scraper = object.__new__(ScrapeTimetable)
        self.scraper.schema = "timetables"
        self.scraper.date = datetime.date(2024, 2, 29)
        self.scraper.sql_timestamp_format = "%Y-%m-%d %H:%M"
        self.scraper.sql_date_format = "%Y-%m-%d"
        self.scraper.recreate_tables = True
        self.scraper.login = mock.sentinel.login

    def test_scrape_runs_timetable_lifecycle(self):
        connection = mock.MagicMock()
        self.scraper.create_timetable_tables = mock.Mock()
        self.scraper.truncate_timetables = mock.Mock()
        self.scraper.get_fahrten_for_stops = mock.Mock()
        self.scraper.add_missing_stops = mock.Mock()

        with mock.patch(
            "extractiontools.scrape_timetable.Connection",
            return_value=connection_context(connection),
        ):
            self.scraper.scrape()

        self.scraper.create_timetable_tables.assert_called_once_with()
        self.scraper.truncate_timetables.assert_called_once_with()
        self.scraper.get_fahrten_for_stops.assert_called_once_with()
        self.scraper.add_missing_stops.assert_called_once_with()
        self.assertEqual(connection.commit.call_count, 3)

    def test_add_journey_writes_departure_and_all_route_sections(self):
        cursor = mock.MagicMock()
        cursor.fetchone.return_value = (42,)
        self.scraper.conn = mock.MagicMock()
        self.scraper.conn.cursor.return_value = cursor
        journey = {
            "url": "https://example.invalid/journey",
            "name": "RE 1",
            "destination": "Destination",
            "number": "1",
        }
        route = [
            {
                "departure": datetime.datetime(2024, 2, 29, 9, 0),
                "arrival": None,
                "station_name": "Origin",
                "station_id": 1,
            },
            {
                "departure": None,
                "arrival": datetime.datetime(2024, 2, 29, 10, 0),
                "station_name": "Destination",
                "station_id": 2,
            },
        ]

        self.scraper.add_journey(journey, route, stop_id=1)

        self.assertEqual(cursor.execute.call_count, 3)
        self.assertIn('INSERT INTO "timetables".abfahrten', cursor.execute.call_args_list[0].args[0])
        self.assertIn('INSERT INTO "timetables".fahrten', cursor.execute.call_args_list[1].args[0])
        self.assertEqual(cursor.execute.call_args_list[1].kwargs["vars"]["journey_id"], 42)

    def test_add_missing_stops_does_not_create_foreign_schema_without_missing_ids(self):
        cursor = mock.MagicMock()
        cursor.fetchall.return_value = []
        self.scraper.conn = mock.MagicMock()
        self.scraper.conn.cursor.return_value = cursor
        self.scraper.create_foreign_schema = mock.Mock()
        self.scraper.cleanup = mock.Mock()

        self.scraper.add_missing_stops()

        self.scraper.create_foreign_schema.assert_not_called()
        self.scraper.cleanup.assert_not_called()


if __name__ == "__main__":
    unittest.main()
