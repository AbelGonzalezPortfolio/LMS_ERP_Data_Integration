"""
Abel Gonzalez 

Development Start Date: 10/11/2021
Development Latest Update Date: 10/11/2021
"""

"""
TODO:
- [] Encrypt SQL server password
"""

from canvasapi import Canvas

import pandas as pd
import sqlalchemy as db

import pyodbc
import requests
import time
import zipfile
import pathlib
import io
import datetime


class Ripon:
    """
    Synchronize Ripon's Canva's data with SIS's respective data.

    The registrar updates users, courses, sections and enrollments
    data through Jenzabar. This updates need to be reflected in Canva's
    database. This module manages both databases in order to facilitate
    synchronization.
    """

    def __init__(self, config):
        self.canvas_connection = Canvas(config('api_url'), config('api_key'))
        self.canvas_admin = self.canvas_connection.get_account(1)
        self._report = {"object": None, "path": None, "courses": None, "enrollments": None,
                        "sections": None, "users": None}

        self.sis_table_names = ["users", "courses", "sections", "enrollments"]


        self.sis_engine = db.create_engine(f"mssql+pyodbc://{config('sis_username')}:{config('sis_password')}@{config('sis_server')}/{config('sis_database')}?driver=SQL+Server+Native+Client+10.0")
        self.sis_term_id = self.get_sis_term()


    @property
    def report(self):
        """
        Get provisioning report object. If it does not exists, create a new one.
        """
        if self._report["object"] is None:
            self.build_provisioning_report()

        return self._report


    def build_provisioning_report(self):
        """
        Create and download provisioning report, assign variables.
        """
        self._report["object"] = self.create_provisioning_report()
        # self._report["object"] = self.get_latest_provisioning_report_object()
        self._report["path"] = self.download_provisioning_report()
        self._report["courses"] = self.get_report_courses()
        self._report["enrollments"] = self.get_report_enrollments()
        self._report["sections"] = self.get_report_sections()
        self._report["users"] = self.get_report_users()

    
    def get_latest_provisioning_report_object(self):
        """
        Get latest provisioning report.
        """
        latest_report = self.canvas_admin.get_index_of_reports("provisioning_csv")[0]
        report = self.canvas_admin.get_report("provisioning_csv", latest_report.id)

        return report


    def sis_import_all_files(self):
        file_names = ["faculty_users.csv", "student_users.csv", "courses.csv", "sections.csv", "enrollments.csv"]

        for name in file_names:
            sis_import = self.sis_import_file(name)
            self.get_sis_import_info(sis_import)


    def sis_import_file(self, file_name):
        print(f"Uploading {file_name} updates to Canvas.")
        data_path = pathlib.Path(r"./data/")
        report_created_at = self.report["object"].created_at.replace(":","_")

        update_path = data_path / f"update_{report_created_at}"

        sis_import = self.canvas_admin.create_sis_import(str(update_path / file_name))

        while self.canvas_admin.get_sis_import(sis_import).progress != 100:
            time.sleep(2)

        sis_import_finished = self.canvas_admin.get_sis_import(sis_import)

        return sis_import_finished


    def get_sis_import_info(self, sis_import):
        print("###################################")
        print("SIS Import Results:")
        print("-------------------")
        print("Changes:")
        try:
            print(sis_import.statistics)
        except AttributeError:
            print("No changes to report.")
        print("-------------------")
        print("Warnings:")
        try:
            print(sis_import.processing_warnings)
        except AttributeError:
            print("No warnings to report.")
        print("-------------------")
        print("Errors:")
        try:
            print(sis_import.processing_errors)
        except AttributeError:
            print("No errors to report.")
        print("###################################")