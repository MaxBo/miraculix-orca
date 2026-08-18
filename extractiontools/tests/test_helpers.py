"""Tests for helper code that does not need databases or source datasets."""

import datetime
import os
import tempfile
import time
import unittest
from unittest import mock

import numpy as np

from extractiontools.archive import Archive
from extractiontools.ausschnitt import BBox, Extract
from extractiontools.connection import DBApp, Login
from extractiontools.start_otp_router import OTPServer
from extractiontools.transit.time_utils import (
    get_timedelta_from_arr,
    timedelta_to_HHMMSS,
    to_datetime,
)
from extractiontools.utils.bahn_query import BahnQuery
from extractiontools.utils.file_in_zipfile import (
    ReadFileInZipfile,
    WriteFileInZipfile,
)
from extractiontools.utils.get_date import Date, get_timestamp2
from extractiontools.utils.htmlentity import htmlentitydecode
from extractiontools.utils.utils import seconds_time_string, time_adder


class TestCoreHelpers(unittest.TestCase):
    def test_bbox_rounding_and_representation(self):
        bbox = BBox(top=54.678, bottom=54.321, left=9.123, right=9.987)

        self.assertEqual(bbox.rounded(2), (9.12, 9.99, 54.32, 54.68))
        self.assertIn("top: 54.68", repr(bbox))

    def test_login_and_dbapp_login_follow_environment(self):
        with mock.patch.dict(
            os.environ,
            {
                "DB_HOST": "database.example",
                "DB_PORT": "5544",
                "DB_USER": "test-user",
                "DB_PASS": "test-password",
            },
            clear=False,
        ):
            app = DBApp()
            app.set_login(database="test-db")

        self.assertEqual(app.login.host, "database.example")
        self.assertEqual(app.login.port, "5544")
        self.assertEqual(app.login.user, "test-user")
        self.assertEqual(app.login.password, "test-password")
        self.assertEqual(app.login.db, "test-db")
        self.assertIn("db=test-db", repr(Login(db="test-db")))

    def test_extract_reads_matching_password_from_pgpass(self):
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as pgpass:
            pgpass.write("localhost:5432:source-db:reader:secret\n")
            pgpass_path = pgpass.name
        self.addCleanup(os.unlink, pgpass_path)

        extract = Extract(destination_db="target-db", target_srid=25832)
        with mock.patch.dict(os.environ, {"PGPASSFILE": pgpass_path}, clear=False):
            password = extract.get_password_from_pgpass(
                find_host="localhost",
                find_port="5432",
                find_db="source-db",
                find_user="reader",
            )

        self.assertEqual(password, "secret")

    def test_archive_helpers_only_list_gzip_files(self):
        with tempfile.TemporaryDirectory() as folder:
            archive = os.path.join(folder, "project.sql.gz")
            ignored = os.path.join(folder, "notes.txt")
            open(archive, "wb").close()
            open(ignored, "wb").close()
            timestamp = 1_700_000_000
            os.utime(archive, (timestamp, timestamp))

            with mock.patch.object(Archive, "archive_folder", folder):
                available = Archive.available_archives()

        self.assertEqual(set(available), {"project.sql.gz"})
        self.assertEqual(
            available["project.sql.gz"],
            datetime.datetime.fromtimestamp(timestamp).strftime("%d.%m.%Y %I:%M:%S"),
        )

    def test_date_parsing_shifting_and_timestamp_formatting(self):
        date = Date.from_string("29.02.2024")

        self.assertEqual(str(date), "29.02.24")
        self.assertEqual(date.shift_day(1), Date(2024, 3, 1))
        self.assertEqual(
            date.get_timestamp(time.struct_time((2024, 1, 1, 9, 5, 2, 0, 1, -1))),
            "2024-02-29 09:05:02CET",
        )
        self.assertIsNone(get_timestamp2(None))

    def test_time_helpers_keep_hours_beyond_midnight(self):
        self.assertEqual(time_adder("23:59:40", "00:01:30"), "24:01:10")
        self.assertEqual(seconds_time_string(3_661), "1:01:01")
        self.assertEqual(to_datetime("01:02:03"), "1900-01-01T01:02:03")
        np.testing.assert_array_equal(
            timedelta_to_HHMMSS(np.array([0, 3_661, 90_061], dtype="timedelta64[s]")),
            np.array(["00:00:00", "01:01:01", "25:01:01"]),
        )
        self.assertTrue(
            np.array_equal(
                get_timedelta_from_arr(np.array(["00:00:00", "01:00:00"])),
                np.array([0, 3_600], dtype="timedelta64[s]"),
            )
        )

    def test_bahn_coordinate_conversion_is_reversible_to_six_decimals(self):
        query = BahnQuery(dt=datetime.date(2024, 1, 1))

        encoded = query._to_db_coord(9.1234567)

        self.assertEqual(encoded, "9123457")
        self.assertEqual(query._from_db_coord(int(encoded)), 9.123457)

    def test_html_entities_are_decoded_from_cp1252_bytes(self):
        self.assertEqual(
            htmlentitydecode(b"Tom &amp; Jerry &apos;Cafe&apos;"),
            "Tom & Jerry 'Cafe'",
        )

    def test_zip_file_context_managers_round_trip_bytes(self):
        with tempfile.TemporaryDirectory() as folder:
            filename = os.path.join(folder, "data.zip")
            with WriteFileInZipfile(filename, "w", "nested/data.txt") as output:
                output.write(b"content")
            with ReadFileInZipfile(filename, "r", "nested/data.txt") as input_file:
                content = input_file.read()

        self.assertEqual(content, b"content")

    def test_otp_server_builds_project_and_graph_paths(self):
        with mock.patch.dict(os.environ, {"HOME": "/home/tester"}, clear=False):
            server = OTPServer(
                ports={"port": 7789, "secure_port": 7788},
                base_path="~/gis/projekte",
                graph_subfolder="graphs",
            )

        self.assertEqual(
            server.get_otp_build_folder("demo", "otp"),
            os.path.join("/home/tester/gis/projekte", "demo", "otp"),
        )
        self.assertEqual(server.graph_folder, os.path.join("/home/tester/gis", "graphs"))

        with mock.patch.object(server, "kill_process_on_port") as kill:
            server.stop()
        kill.assert_has_calls([mock.call(7789), mock.call(7788)])


if __name__ == "__main__":
    unittest.main()
