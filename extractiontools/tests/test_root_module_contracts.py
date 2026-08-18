"""Database-free contract tests for root-level extraction modules."""

import datetime
import importlib
import os
import sys
import tempfile
import types
import unittest
from unittest import mock

from extractiontools.drop_db import DropDatabase
from extractiontools.hafas2gtfs import Hafas2GTFS
from extractiontools.raster_from_points import PixelType, Points2Raster
from extractiontools.transit.netToGtf_new import KNOTEN, Table, to_float
from extractiontools.utils.get_date import Date


def load_hafasdb_module():
    """Load the converter without requiring the optional Orca runtime."""

    fake_orca = types.ModuleType("orca")
    fake_orca.logger = mock.sentinel.orca_logger
    module_name = "extractiontools.hafasdb2gtfs"
    sys.modules.pop(module_name, None)
    with mock.patch.dict(sys.modules, {"orca": fake_orca}):
        return importlib.import_module(module_name)


def load_hafas_text_module():
    """Load legacy helper functions without their unused graphserver writer."""

    graphserver_tools = types.ModuleType("graphserver_tools")
    graphserver_tools.__path__ = []
    graphserver_utils = types.ModuleType("graphserver_tools.utils")
    graphserver_utils.__path__ = []
    utf8csv = types.ModuleType("graphserver_tools.utils.utf8csv")
    utf8csv.UnicodeWriter = object
    module_name = "extractiontools.transit.hafasToGtf"
    sys.modules.pop(module_name, None)
    with mock.patch.dict(
        sys.modules,
        {
            "graphserver_tools": graphserver_tools,
            "graphserver_tools.utils": graphserver_utils,
            "graphserver_tools.utils.utf8csv": utf8csv,
        },
    ):
        return importlib.import_module(module_name)


class TestHafas2Gtfs(unittest.TestCase):
    def setUp(self):
        self.converter = Hafas2GTFS(schema="timetables", day=29, month=2, year=2024)

    def test_constructor_exposes_requested_service_day(self):
        self.assertEqual(self.converter.today, datetime.datetime(2024, 2, 29))
        self.assertEqual(self.converter.day, 29)

    def test_make_stop_times_uses_service_day_in_generated_sql(self):
        self.converter.run_query = mock.Mock()

        self.converter.make_stop_times()

        sql = self.converter.run_query.call_args.args[0]
        self.assertIn("date_part('day', \"H_Ankunft\")= 29", sql)
        self.assertIn("TRUNCATE gtfs_stop_times", sql)

    def test_convert_runs_all_transformations_in_order(self):
        connection = mock.MagicMock()
        method_names = (
            "set_search_path",
            "fill_gtfs_stops",
            "identify_kreis",
            "line_type",
            "make_routes",
            "make_agencies",
            "make_shapes",
            "make_stop_times",
        )
        for name in method_names:
            setattr(self.converter, name, mock.Mock())

        with mock.patch("extractiontools.hafas2gtfs.Connection") as connection_class:
            connection_class.return_value.__enter__.return_value = connection
            self.converter.convert()

        self.assertEqual([getattr(self.converter, name).call_count for name in method_names], [1] * len(method_names))
        connection.commit.assert_called_once_with()

    def test_export_gtfs_exports_all_required_tables_to_given_folder(self):
        connection = mock.MagicMock()
        cursor = connection.cursor.return_value
        connection.copy_sql = "COPY {tn} TO STDOUT;"
        self.converter.set_search_path = mock.Mock()

        def write_export(_sql, file_object):
            file_object.write("test data\n")

        cursor.copy_expert.side_effect = write_export
        with tempfile.TemporaryDirectory() as folder, mock.patch(
            "extractiontools.hafas2gtfs.Connection"
        ) as connection_class:
            connection_class.return_value.__enter__.return_value = connection
            self.converter.export_gtfs(folder)

            exported = sorted(os.listdir(folder))

        self.assertEqual(
            exported,
            sorted(
                [
                    "agency.txt", "calendar.txt", "calendar_dates.txt", "routes.txt",
                    "shapes.txt", "stop_times.txt", "stops.txt", "transfers.txt", "trips.txt",
                ]
            ),
        )
        self.assertEqual(cursor.copy_expert.call_count, 9)
        cursor.execute.assert_has_calls(
            [
                mock.call("SET CLIENT_ENCODING TO 'LATIN9';"),
                mock.call("RESET CLIENT_ENCODING;"),
            ]
        )


class TestHafasDb2Gtfs(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.module = load_hafasdb_module()

    def test_constructor_derives_target_table_and_service_date(self):
        with mock.patch.object(self.module.HafasDB2GTFS, "get_target_srid", return_value=25832):
            converter = self.module.HafasDB2GTFS(
                db="project",
                date=datetime.date(2024, 2, 29),
                only_one_day=True,
                base_path="/exports",
                subfolder="otp",
                tbl_kreise="kreise",
            )

        self.assertEqual(converter.today, Date(2024, 2, 29))
        self.assertEqual(converter.tbl_kreise, "verwaltungsgrenzen.kreise")
        self.assertEqual(converter.target_srid, 25832)

    def test_write_calendar_generates_single_day_or_permanent_service(self):
        converter = object.__new__(self.module.HafasDB2GTFS)
        converter.today = Date(2024, 2, 29)
        converter.run_query = mock.Mock()

        converter.only_one_day = True
        converter.write_calendar()
        single_day_sql = converter.run_query.call_args_list[-1].args[0]
        self.assertIn("20240229", single_day_sql)
        self.assertIn("20240301", single_day_sql)

        converter.only_one_day = False
        converter.write_calendar()
        permanent_sql = converter.run_query.call_args_list[-1].args[0]
        self.assertIn("20000101,20991231", permanent_sql)

    def test_create_aggregate_functions_only_creates_missing_aggregate(self):
        converter = object.__new__(self.module.HafasDB2GTFS)
        converter.conn = mock.MagicMock()
        converter.conn.cursor.return_value.rowcount = 0
        converter.run_query = mock.Mock()

        converter.create_aggregate_functions()

        self.assertIn("CREATE AGGREGATE public.array_accum", converter.run_query.call_args.args[0])


class TestRasterContracts(unittest.TestCase):
    def test_pixel_type_accepts_supported_values_and_rejects_unknown_values(self):
        pixel_type = PixelType("32BF")

        self.assertEqual(pixel_type.pixeltype, "32BF")
        self.assertEqual(pixel_type.description, "32-bit float")
        with self.assertRaisesRegex(ValueError, "pixeltype UNKNOWN not defined"):
            PixelType("UNKNOWN")

    def test_point_to_raster_generates_raster_sql_and_index_request(self):
        converter = object.__new__(Points2Raster)
        converter.logger = mock.Mock()
        converter.conn = mock.sentinel.connection
        converter.run_query = mock.Mock()
        converter.add_raster_index = mock.Mock()

        converter.point2raster(
            point_feature="source.points",
            geom_col="geom",
            value_col="value",
            target_raster="laea.population",
            pixeltype="32BF",
            srid=3035,
            overwrite=True,
        )

        self.assertIn("DROP TABLE IF EXISTS laea.population CASCADE", converter.run_query.call_args_list[0].args[0])
        create_sql = converter.run_query.call_args_list[1].args[0]
        self.assertIn("CREATE TABLE IF NOT EXISTS laea.population", create_sql)
        self.assertIn("'32BF'::text", create_sql)
        self.assertIn("source.points", create_sql)
        converter.add_raster_index.assert_called_once_with(
            schema="laea",
            tablename="population",
            raster_column="rast",
            conn=mock.sentinel.connection,
        )


class TestDropDatabase(unittest.TestCase):
    def setUp(self):
        self.dropper = DropDatabase(destination_db="project", target_srid=25832)

    def test_missing_database_does_not_open_admin_connection(self):
        self.dropper.set_pg_path = mock.Mock()
        self.dropper.check_if_database_exists = mock.Mock(return_value=False)

        self.dropper.extract()

        self.dropper.set_pg_path.assert_called_once_with()
        self.dropper.check_if_database_exists.assert_called_once_with("project")

    def test_existing_database_is_dropped_using_postgres_connection(self):
        self.dropper.set_pg_path = mock.Mock()
        self.dropper.check_if_database_exists = mock.Mock(return_value=True)
        self.dropper.drop_database = mock.Mock()
        connection = mock.MagicMock()

        with mock.patch("extractiontools.drop_db.Connection") as connection_class:
            connection_class.return_value.__enter__.return_value = connection
            self.dropper.extract()

        admin_login = connection_class.call_args.kwargs["login"]
        self.assertEqual(admin_login.db, "postgres")
        self.dropper.drop_database.assert_called_once_with(dbname="project", conn=connection)
        connection.commit.assert_called_once_with()


class TestVisumAndHafasTextParsing(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.hafas = load_hafas_text_module()

    def test_hafas_bitfield_and_date_helpers(self):
        self.assertEqual(self.hafas.hex_to_bool_list("0Af", verbose=False), [False, False, False, False, True, False, True, False, True, True, True, True])
        with self.assertRaisesRegex(Exception, "unrecognizable character"):
            self.hafas.hex_to_bool_list("G", verbose=False)
        self.assertEqual(self.hafas.increment_date_string("20240229"), "20240301")

    def test_visum_table_parses_inline_rows_and_column_formats(self):
        table = Table("$TABLE:NR;NAME")
        table.add_row("1;Central")

        self.assertEqual(table.get_row(("1",)).NAME, "Central")
        self.assertEqual(to_float("8,123"), 8.123)

        nodes = KNOTEN("$KNOTEN:NR;XKOORD;YKOORD")
        nodes.add_row("7;8,123;50,456")
        node = nodes.get_row((7,))
        self.assertEqual(node.NR, 7)
        self.assertEqual(node.XKOORD, 8.123)
        self.assertEqual(node.YKOORD, 50.456)


if __name__ == "__main__":
    unittest.main()
