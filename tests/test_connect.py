from src.ripon import Ripon
from canvasapi import Canvas
from decouple import AutoConfig
from pathlib import Path

import pandas as pd

import warnings
import datetime
import unittest

class TestCanvasApi(unittest.TestCase):
    """
    Test Canvas Api functions.
    """
    def setUp(self):
        """
        Setup ripon instance from Ripon class to be tested.
        """
        warnings.simplefilter("ignore", ResourceWarning)
        current_path = Path(__file__)

        config = AutoConfig(search_path=current_path.parents[1])

        self.ripon = Ripon(config)


    def test_connection(self):
        """
        Test Canvas API connection is successful.
        """
        self.assertIsInstance(self.ripon.canvas_connection, Canvas)


    def test_get_current_canvas_term(self):
        """
        Test that current canvas term is returned.
        """
        expected_canvas_term_id = 42
        expected_sis_term_id = "211S"

        self.assertEqual(self.ripon.get_current_canvas_term().id, expected_canvas_term_id)
        self.assertEqual(self.ripon.get_current_canvas_term().sis_term_id, expected_sis_term_id)


    def test_create_provisioning_report(self):
        """
        Test that create_provisioning_report creates a new report.
        """
        all_reports = self.ripon.canvas_admin.get_index_of_reports("provisioning_csv")
        latest_report = all_reports[0]

        new_report = self.ripon.report["object"]

        expected_report_id = latest_report.id + 1
        expected_report_name = "provisioning_csv"
        expected_report_status = "complete"
        
        self.assertEqual(new_report.id, expected_report_id)
        self.assertEqual(new_report.report, expected_report_name)
        self.assertEqual(new_report.status, expected_report_status)


    def test_get_latest_provisioning_report_object(self):
        """
        Test that get_latest_provisioning_report_object returns the latest report.
        """
        actual_report = self.ripon.get_latest_provisioning_report_object()

        latest_report = None
        latest_report_date = datetime.datetime.min

        for report in self.ripon.canvas_admin.get_index_of_reports("provisioning_csv"):
            report_date_string = report.ended_at.rsplit('-', 1)[0]
            report_date = datetime.datetime.strptime(report_date_string, "%Y-%m-%dT%H:%M:%S")

            if report_date > latest_report_date:
                latest_report = report
                latest_report_date = report_date

        expected_report_id = latest_report.id
        expected_report_name = latest_report.report
        expected_report_started_at = latest_report.started_at
        expected_report_created_at = latest_report.created_at
        expected_report_ended_at = latest_report.ended_at

        self.assertEqual(actual_report.id, expected_report_id)
        self.assertEqual(actual_report.report, expected_report_name)
        self.assertEqual(actual_report.started_at, expected_report_started_at)
        self.assertEqual(actual_report.created_at, expected_report_created_at)
        self.assertEqual(actual_report.ended_at, expected_report_ended_at)


    def test_download_provisioning_report(self):
        """
        Test the download of report files.
        """
        expected_created_at = self.ripon.get_latest_provisioning_report_object().created_at.replace(":","_")
        expected_path = Path("./data/"+f"prov_{expected_created_at}")

        actual_path = self.ripon.report["path"]

        self.assertEqual(actual_path, expected_path)

        self.assertTrue(Path(expected_path / "enrollments.csv").is_file())
        self.assertTrue(Path(expected_path / "courses.csv").is_file())
        self.assertTrue(Path(expected_path / "users.csv").is_file())
        self.assertTrue(Path(expected_path / "sections.csv").is_file())


    def test_get_report_users(self):
        """
        Test that Users report data is ready for SQL upload.
        """
        users = self.ripon.report["users"] 

        expected_columns = ["id_num", "canvas_user", "login_id", "load_date"]

        self.assertIsInstance(users, pd.DataFrame)
        self.assertTrue(len(list(users)), len(expected_columns))
        for column in list(users):
            self.assertIn(column, expected_columns)

        self.assertEqual(self.ripon.report["users"]["login_id"].str.contains("sdemo+").sum(), 0)


    def test_get_report_courses(self):
        """
        Test that Courses report is ready for SQL upload.
        """
        courses = self.ripon.report["courses"]

        expected_columns = ["yr_cde", "trm_cde", "crs_cde", "canvas_course_id", "load_date", "status"]

        self.assertTrue(len(list(courses)), len(expected_columns))
        for column in list(courses):
            self.assertIn(column, expected_columns)


    def test_get_report_sections(self):
        """
        Test that Sections report is ready for SQL upload.
        """
        sections = self.ripon.report["sections"]

        expected_columns = ["yr_cde", "trm_cde", "crs_cde", "section_id", "name", "status",
                            "account_id", "canvas_section_id", "created_by_sis", "load_date"]

        self.assertTrue(len(list(sections)), len(expected_columns))
        for column in list(sections):
            self.assertIn(column, expected_columns)


    def test_get_report_enrollments(self):
        """
        Test that Enrollments report is ready for SQL upload.
        """
        enrollments = self.ripon.report["enrollments"]

        expected_columns = ["yr_cde", "trm_cde", "course_id", "user_id", "role", "section_id", "status",
                            "canvas_enrollment_id", "canvas_section_id", "created_by_sis", "load_date"]

        self.assertTrue(len(list(enrollments)), len(expected_columns))
        for column in list(enrollments):
            self.assertIn(column, expected_columns)


    def test_prepare_dataframe_for_sql(self):
        """
        Test that dataframe is prepared to uload to sql.
        """
        df = pd.DataFrame({"A":[1,2,3], "B":[4,5,6], "created_by_sis":[True, True, False]})

        cols_to_keep = ["A", "created_by_sis"]
        cols_name_map = {"A": "new_A"}

        df = self.ripon._prepare_dataframe_for_sql(df, cols_to_keep, cols_name_map, True, True)

        self.assertNotIn("B", list(df))

        self.assertIn("new_A", list(df))
        self.assertNotIn("A", list(df))

        self.assertEqual(df["created_by_sis"][0], -1)
        self.assertEqual(df["created_by_sis"][1], -1)
        self.assertEqual(df["created_by_sis"][2], 0)


    def test_upload_dataset_to_sis(self):
        """
        Test a given dataframe uploads to a given SIS table name. 
        """
        expected_enrollments = self.ripon.report["enrollments"]

        self.ripon.upload_dataset_to_sis(expected_enrollments, "rpc_RE_Canvas_Enrollments")

        with self.ripon.sis_engine.connect() as conn:
            actual_enrollments = pd.read_sql("rpc_RE_Canvas_Enrollments", conn)

        self.assertEqual(actual_enrollments.size, expected_enrollments.size)


    # def test_upload_datasets_to_sis(self):
    #     """
    #     Test that multiple dataframes upload to corresponding sis tables.
    #     """
    #     expected_users = self.ripon.report["users"]
    #     expected_courses = self.ripon.report["courses"]
    #     expected_sections = self.ripon.report["sections"]
    #     expected_enrollments = self.ripon.report["enrollments"]

    #     self.ripon.upload_full_report_to_sis(taget_source_map)

    #     actual_enrollments = pd.read_sql("rpc_RE_Canvas_Enrollments", self.ripon.sis_connection)
    #     actual_users = pd.read_sql("rpc_RE_Canvas_Users", self.ripon.sis_connection)
    #     actual_courses = pd.read_sql("rpc_RE_Canvas_Courses", self.ripon.sis_connection)
    #     actual_sections = pd.read_sql("rpc_RE_Canvas_Sections", self.ripon.sis_connection)

    #     self.assertEqual(actual_enrollments.size, expected_enrollments.size)
    #     self.assertEqual(actual_users.size, expected_users.size)
    #     self.assertEqual(actual_courses.size, expected_enrollments.size)
    #     self.assertEqual(actual_sections.size, expected_users.size)







