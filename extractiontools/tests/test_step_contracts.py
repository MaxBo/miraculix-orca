"""Contract tests for Orca steps without invoking their external workers."""

import importlib
import os
import sys
import tempfile
import types
import unittest
from datetime import date
from unittest import mock


def _identity_decorator(*args, **kwargs):
    """Provide the small decorator interface needed to import a step module."""

    return lambda function: function


def load_step_module(module_name, *, needs_gtfs_kit=False):
    """Import a step module without requiring Orca during unit tests."""

    fake_orca = types.ModuleType("orca")
    fake_orca.step = _identity_decorator
    fake_orca.injectable = _identity_decorator
    fake_orca.logger = mock.sentinel.orca_logger

    fake_orcadjango = types.ModuleType("orcadjango")
    fake_orcadjango.__path__ = []
    fake_decorators = types.ModuleType("orcadjango.decorators")
    fake_decorators.meta = _identity_decorator

    replacements = {
        "orca": fake_orca,
        "orcadjango": fake_orcadjango,
        "orcadjango.decorators": fake_decorators,
    }
    if needs_gtfs_kit:
        replacements["gtfs_kit"] = types.ModuleType("gtfs_kit")

    sys.modules.pop(module_name, None)
    with mock.patch.dict(sys.modules, replacements):
        return importlib.import_module(module_name)


class TestCreateDatabaseSteps(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.steps = load_step_module("extractiontools.steps.create_db")

    def test_create_db_constructs_and_initializes_extract(self):
        project_area = object()
        with mock.patch.object(self.steps, "Extract") as extract_class:
            worker = extract_class.return_value

            self.steps.create_db("25832", project_area, "project", {})

        extract_class.assert_called_once_with(
            destination_db="project",
            target_srid="25832",
            logger=mock.sentinel.orca_logger,
        )
        worker.recreate_db.assert_called_once_with()
        worker.set_target_boundary.assert_called_once_with(project_area)
        worker.update_boundaries.assert_called_once_with()

    def test_archive_db_optionally_drops_database_after_archiving(self):
        with (
            mock.patch.object(self.steps, "Archive") as archive_class,
            mock.patch.object(self.steps, "DropDatabase") as drop_class,
        ):
            self.steps.archive_db("project", {}, True)

        archive_class.assert_called_once_with(database="project", logger=mock.sentinel.orca_logger)
        archive_class.return_value.archive.assert_called_once_with()
        drop_class.assert_called_once_with(destination_db="project", logger=mock.sentinel.orca_logger)
        drop_class.return_value.extract.assert_called_once_with()

    def test_restore_db_translates_default_archive_to_empty_filename(self):
        with mock.patch.object(self.steps, "Archive") as archive_class:
            self.steps.restore_db("project", {}, "default")

        archive_class.assert_called_once_with(database="project", archive_fn="", logger=mock.sentinel.orca_logger)
        archive_class.return_value.unarchive.assert_called_once_with()

    def test_grant_access_delegates_to_extract(self):
        with mock.patch.object(self.steps, "Extract") as extract_class:
            self.steps.grant_access("project", ["analyst", "viewer"])

        extract_class.assert_called_once_with(destination_db="project", logger=mock.sentinel.orca_logger)
        extract_class.return_value.grant_access.assert_called_once_with(["analyst", "viewer"])

    def test_drop_db_delegates_to_drop_database_worker(self):
        with mock.patch.object(self.steps, "DropDatabase") as worker_class:
            self.steps.drop_db("project", {})

        worker_class.assert_called_once_with(destination_db="project", logger=mock.sentinel.orca_logger)
        worker_class.return_value.extract.assert_called_once_with()


class TestExtractionDataSteps(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.steps = load_step_module("extractiontools.steps.extract_data")

    def test_extract_osm_passes_source_target_and_boundary(self):
        boundary = object()
        with mock.patch.object(self.steps, "ExtractOSM") as worker_class:
            self.steps.extract_osm("europe", "project", 25832, boundary)

        worker_class.assert_called_once_with(
            source_db="europe",
            destination_db="project",
            target_srid=25832,
            logger=mock.sentinel.orca_logger,
            boundary=boundary,
        )
        worker_class.return_value.extract.assert_called_once_with()

    def test_geometry_extractors_build_table_mapping(self):
        boundary = object()
        cases = (
            (
                "extract_verwaltungsgrenzen",
                "ExtractVerwaltungsgrenzen",
                ("europe", "project", ["gem", "krs"], 25832, boundary),
            ),
            (
                "extract_firms_neighbourhoods",
                "ExtractFirmsNeighbourhoods",
                ("europe", "project", ["firms", "neighbourhoods"], 25832, boundary),
            ),
        )
        for step_name, class_name, args in cases:
            with self.subTest(step=step_name), mock.patch.object(self.steps, class_name) as worker_class:
                getattr(self.steps, step_name)(*args)

            worker_class.assert_called_once_with(
                source_db="europe",
                destination_db="project",
                tables={name: "geom" for name in args[2]},
                logger=mock.sentinel.orca_logger,
                boundary=boundary,
            )
            worker_class.return_value.extract.assert_called_once_with()

    def test_landuse_and_laea_steps_delegate_to_their_workers(self):
        boundary = object()
        with mock.patch.object(self.steps, "ExtractLanduse") as landuse_class:
            self.steps.extract_landuse("europe", "project", ["ua2018"], ["clc18"], 25832, boundary)
        landuse_class.assert_called_once_with(
            source_db="europe",
            destination_db="project",
            gmes=["ua2018"],
            corine=["clc18"],
            target_srid=25832,
            logger=mock.sentinel.orca_logger,
            boundary=boundary,
        )
        landuse_class.return_value.extract.assert_called_once_with()

        with (
            mock.patch.object(self.steps, "ExtractLAEA") as laea_class,
            mock.patch.object(self.steps, "Zensus2Raster") as zensus_class,
        ):
            self.steps.extract_laea_raster("europe", "project", 25832, boundary)
        laea_class.assert_called_once_with(
            source_db="europe",
            destination_db="project",
            logger=mock.sentinel.orca_logger,
            boundary=boundary,
        )
        laea_class.return_value.extract.assert_called_once_with()
        zensus_class.assert_called_once_with(db="project", logger=mock.sentinel.orca_logger)
        zensus_class.return_value.run.assert_called_once_with()

    def test_create_polygons_step_uses_polygon_worker(self):
        with mock.patch.object(self.steps, "CreatePolygons") as worker_class:
            self.steps.create_polygons_from_osm("project")

        worker_class.assert_called_once_with(destination_db="project", logger=mock.sentinel.orca_logger)
        worker_class.return_value.create_poly_and_multipolygons.assert_called_once_with()

    def test_osm_export_steps_use_expected_layer_configuration_and_format(self):
        layers = {"railways": "osm_layer"}
        with mock.patch.object(self.steps, "CopyOSM2FGDB") as worker_class:
            self.steps.create_osm_views("project")

        worker_class.assert_called_once_with(
            destination_db="project",
            layers=self.steps.osm_layers(),
            filename="osm_layers.gdb",
            schema="osm_layer",
            logger=mock.sentinel.orca_logger,
        )
        worker_class.return_value.create_views.assert_called_once_with()

        for step_name, expected_format in (("copy_osm_to_fgdb", "OpenFileGDB"), ("copy_osm_to_gpkg", "GPKG")):
            with self.subTest(step=step_name), mock.patch.object(self.steps, "CopyOSM2FGDB") as worker_class:
                getattr(self.steps, step_name)("project", layers)
            worker_class.assert_called_once_with(
                destination_db="project",
                layers=layers,
                filename="osm_layers",
                schema="osm_layer",
                logger=mock.sentinel.orca_logger,
            )
            worker_class.return_value.copy_layers.assert_called_once_with(expected_format)

    def test_statistical_and_pendler_steps_use_their_configured_arguments(self):
        cases = (
            (
                "extract_regionalstatistik",
                "ExtractRegionalstatistik",
                ("europe", "project", "gem", [2020, 2021]),
                "extract",
                {
                    "source_db": "europe",
                    "destination_db": "project",
                    "regionalstatistik_gemeinden": "gem",
                    "regionalstatistik_years": [2020, 2021],
                    "logger": mock.sentinel.orca_logger,
                },
            ),
            (
                "import_pendlerdaten",
                "ImportPendlerdaten",
                ("europe", "Pendlerdaten", ["2020"]),
                "run",
                {
                    "db": "europe",
                    "subfolder": "Pendlerdaten",
                    "pendlerdaten_years": ["2020"],
                    "logger": mock.sentinel.orca_logger,
                },
            ),
            (
                "extract_pendlerdaten",
                "ExtractPendler",
                ("europe", "project", "gem"),
                "extract",
                {
                    "source_db": "europe",
                    "destination_db": "project",
                    "pendlerdaten_gemeinden": "gem",
                    "logger": mock.sentinel.orca_logger,
                },
            ),
            (
                "create_pendlerspinne",
                "CreatePendlerSpinne",
                ("project", "areas", 25832),
                "run",
                {
                    "db": "project",
                    "pendlerspinne_gebiete": "areas",
                    "target_srid": 25832,
                    "logger": mock.sentinel.orca_logger,
                },
            ),
            (
                "export_pendlerdaten",
                "ExportPendlerdaten",
                ("project",),
                "export",
                {"db": "project", "logger": mock.sentinel.orca_logger},
            ),
        )
        for step_name, class_name, args, method, expected_kwargs in cases:
            with self.subTest(step=step_name), mock.patch.object(self.steps, class_name) as worker_class:
                getattr(self.steps, step_name)(*args)
            worker_class.assert_called_once_with(**expected_kwargs)
            getattr(worker_class.return_value, method).assert_called_once_with()

    def test_remaining_extraction_steps_delegate_to_workers(self):
        cases = (
            (
                "gebietsstaende",
                "Gebietsstaende",
                ("project", "reference", ["old"], 1_000),
                "calc",
                {
                    "destination_db": "project",
                    "ref_table": "reference",
                    "comp_tables": ["old"],
                    "threshold": 1_000,
                    "logger": mock.sentinel.orca_logger,
                },
            ),
            (
                "copy_zensus_to_tiff",
                "ExportZensus",
                ("project", "tiffs"),
                "run",
                {"db": "project", "subfolder": "tiffs", "logger": mock.sentinel.orca_logger},
            ),
            (
                "prepare_verschneidungstool",
                "PrepareVerschneidungstool",
                ("europe", "project", 25832),
                "extract",
                {"source_db": "europe", "destination_db": "project", "logger": mock.sentinel.orca_logger},
            ),
            (
                "extract_bast",
                "ExtractBASt",
                ("europe", "project", 25832),
                "extract",
                {"source_db": "europe", "destination_db": "project", "logger": mock.sentinel.orca_logger},
            ),
        )
        for step_name, class_name, args, method, expected_kwargs in cases:
            with self.subTest(step=step_name), mock.patch.object(self.steps, class_name) as worker_class:
                getattr(self.steps, step_name)(*args)
            worker_class.assert_called_once_with(**expected_kwargs)
            getattr(worker_class.return_value, method).assert_called_once_with()


class TestNetworkSteps(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.steps = load_step_module("extractiontools.steps.network", needs_gtfs_kit=True)

    def test_network_build_steps_pass_configuration_to_workers(self):
        cases = (
            (
                "build_network_car",
                "BuildNetwork",
                ("project", 1_000, 0, 10, "clc18", "network"),
                {
                    "db": "project", "network_schema": "network", "limit": 0,
                    "chunksize": 1_000, "links_to_find": 10, "corine": "clc18",
                    "logger": mock.sentinel.orca_logger,
                },
            ),
            (
                "build_network_fr",
                "BuildNetworkWalkCycle",
                ("project", True, 1_000, 0, 10, "clc18", True, "network_fr", "area"),
                {
                    "db": "project", "network_schema": "network_fr", "limit": 0,
                    "chunksize": 1_000, "links_to_find": 10, "corine": "clc18",
                    "routing_walk": True, "include_planning": True,
                    "detailed_network_area": "area", "logger": mock.sentinel.orca_logger,
                },
            ),
        )
        for step_name, class_name, args, expected_kwargs in cases:
            with self.subTest(step=step_name), mock.patch.object(self.steps, class_name) as worker_class:
                getattr(self.steps, step_name)(*args)
            worker_class.assert_called_once_with(**expected_kwargs)
            worker_class.return_value.build.assert_called_once_with()

        with mock.patch.object(self.steps, "BuildGraduatedNetwork") as worker_class:
            self.steps.build_graduated_network_car(
                "project", True, 1_000, 0, 10, "clc18", "network_grad", "detail", "larger"
            )
        worker_class.assert_called_once_with(
            db="project", network_schema="network_grad", limit=0, chunksize=1_000,
            links_to_find=10, include_planning=True, corine="clc18",
            detailed_network_area="detail", larger_network_area="larger",
            logger=mock.sentinel.orca_logger,
        )
        worker_class.return_value.build.assert_called_once_with()

    def test_gtfs_step_builds_input_and_output_paths(self):
        area = object()
        with mock.patch.object(self.steps, "ExtractGTFS") as worker_class, mock.patch.object(
            self.steps, "GTFS_DIR", "/feeds"
        ):
            self.steps.extract_gtfs("project", "/exports", "otp", "input.zip", True, False, area)

        worker_class.assert_called_once_with(
            area, os.path.join("/feeds", "input.zip"), os.path.join("/exports", "project", "otp"),
            do_visum_postproc=True, do_transferprocessing=False,
            logger=mock.sentinel.orca_logger,
        )
        worker_class.return_value.extract.assert_called_once_with()

    def test_public_transport_steps_pass_arguments_to_their_workers(self):
        cases = (
            (
                "extract_stops", "ScrapeStops", ("project",), "extract",
                mock.call("project", logger=mock.sentinel.orca_logger),
            ),
            (
                "scrape_stops", "ScrapeStops", ("project",), "scrape",
                mock.call(destination_db="project", logger=mock.sentinel.orca_logger),
            ),
            (
                "scrape_db_fastest_routes", "DBRouting",
                ("project", "destinations", date(2024, 1, 2), [9, 17], 5_000), "scrape",
                mock.call("project", date(2024, 1, 2), [9, 17], logger=mock.sentinel.orca_logger),
            ),
            (
                "scrape_timetables", "ScrapeTimetable",
                ("project", "europe", date(2024, 1, 2), True), "scrape",
                mock.call(destination_db="project", source_db="europe", date=date(2024, 1, 2),
                          recreate_tables=True, logger=mock.sentinel.orca_logger),
            ),
        )
        for step_name, class_name, args, method, expected_call in cases:
            with self.subTest(step=step_name), mock.patch.object(self.steps, class_name) as worker_class:
                getattr(self.steps, step_name)(*args)
            self.assertEqual(worker_class.call_args, expected_call)
            if step_name == "scrape_db_fastest_routes":
                worker_class.return_value.scrape.assert_called_once_with("destinations", max_distance=5_000)
            else:
                getattr(worker_class.return_value, method).assert_called_once_with()

        with mock.patch.object(self.steps, "HafasDB2GTFS") as worker_class:
            self.steps.timetables_gtfs("project", date(2024, 1, 2), True, "/exports", "otp", "kreise")
        worker_class.assert_called_once_with(
            db="project", date=date(2024, 1, 2), only_one_day=True,
            base_path="/exports", subfolder="otp", tbl_kreise="kreise",
            logger=mock.sentinel.orca_logger,
        )
        worker_class.return_value.convert.assert_called_once_with()
        worker_class.return_value.export_gtfs.assert_called_once_with()

    def test_network_exports_create_one_worker_per_configured_network(self):
        networks = {"network": "otp_car", "network_fr": "otp_fr"}
        cases = (
            ("copy_network_pbf", "CopyNetwork2Pbf", {}),
            ("copy_network_pbf_xml", "CopyNetwork2Pbf", {"as_xml": True}),
            ("copy_tagged_nw_pbf_xml", "CopyNetwork2PbfTagged", {"as_xml": True}),
        )
        for step_name, class_name, extra_kwargs in cases:
            with self.subTest(step=step_name), mock.patch.object(self.steps, class_name) as worker_class:
                getattr(self.steps, step_name)("project", networks)
            self.assertEqual(worker_class.call_count, 2)
            self.assertEqual(
                worker_class.call_args_list,
                [
                    mock.call("project", network_schema="network", subfolder_pbf="otp_car", logger=mock.sentinel.orca_logger, **extra_kwargs),
                    mock.call("project", network_schema="network_fr", subfolder_pbf="otp_fr", logger=mock.sentinel.orca_logger, **extra_kwargs),
                ],
            )
            self.assertEqual(worker_class.return_value.copy.call_count, 2)

    def test_otp_server_steps_delegate_start_stop_and_router_creation(self):
        ports = {"port": 7789, "secure_port": 7788}
        routers = {"project_car": "otp_car"}
        with mock.patch.object(self.steps, "OTPServer") as server_class:
            self.steps.start_otp_router(ports, "/exports", "graphs", routers, False)
        server_class.assert_called_once_with(
            ports=ports, base_path="/exports", graph_subfolder="graphs", routers=routers,
            start_analyst=False, logger=mock.sentinel.orca_logger,
        )
        server_class.return_value.start.assert_called_once_with()

        with mock.patch.object(self.steps, "OTPServer") as server_class:
            self.steps.stop_otp_router(ports)
        server_class.assert_called_once_with(ports=ports, logger=mock.sentinel.orca_logger)
        server_class.return_value.stop.assert_called_once_with()

        with mock.patch.object(self.steps, "OTPServer") as server_class:
            server_class.return_value.graph_folder = "/exports/graphs"
            server_class.return_value.get_otp_build_folder.return_value = "/exports/project/otp_car"
            self.steps.create_router(routers, "project", "/exports")
        server_class.assert_called_once_with(
            ports=None, base_path="/exports", graph_subfolder="otp_graphs", routers=routers,
            logger=mock.sentinel.orca_logger,
        )
        server_class.return_value.create_router.assert_called_once_with(
            "/exports/project/otp_car", os.path.join("/exports/graphs", "project_car")
        )

    def test_network_fgdb_exports_and_static_defaults(self):
        layers = {"links": "network_car"}
        with mock.patch.object(self.steps, "Copy2FGDB") as worker_class:
            self.steps.copy_network_car_fgdb("project", layers)
        worker_class.assert_called_once_with(
            "project", layers=layers, filename="network_car.gdb", schema="network",
            logger=mock.sentinel.orca_logger,
        )
        worker_class.return_value.copy_layers.assert_called_once_with("OpenFileGDB")

        with mock.patch.object(self.steps, "Copy2FGDB") as worker_class:
            self.steps.copy_network_fr_fgdb("project", layers)
        worker_class.assert_called_once_with(
            "project", layers=layers, filename="network_fr.gdb", schema="network_fr",
            logger=mock.sentinel.orca_logger,
        )
        worker_class.return_value.copy_layers.assert_called_once_with("OpenFileGDB")

        self.assertEqual(self.steps.otp_routers("project"), {"project_car": "otp_car", "project_fr": "otp_fr"})
        self.assertEqual(self.steps.gdbname("project"), "project.gdb")
        self.assertTrue(self.steps.gtfs_postprocessing())
        self.assertTrue(self.steps.gtfs_transferprocessing())

        with tempfile.TemporaryDirectory() as folder, mock.patch.object(self.steps, "GTFS_DIR", folder):
            open(os.path.join(folder, "a.zip"), "wb").close()
            open(os.path.join(folder, "ignore.txt"), "wb").close()
            self.assertEqual(self.steps.local_gtfs_files(), ["a.zip"])


class TestGoogleStep(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.steps = load_step_module("extractiontools.steps.google")

    def test_google_places_passes_ui_configuration_to_google_app(self):
        boundary = object()
        with mock.patch.object(self.steps, "GoogleApp") as worker_class:
            self.steps.google_places(
                "project", "api-key", "places", "cafe", "coffee", boundary, 2_000
            )

        worker_class.assert_called_once_with("project", boundary=boundary)
        worker_class.return_value.get_places.assert_called_once_with(
            "api-key", keyword="coffee", typ="cafe", table="places", search_radius=2_000
        )


if __name__ == "__main__":
    unittest.main()
