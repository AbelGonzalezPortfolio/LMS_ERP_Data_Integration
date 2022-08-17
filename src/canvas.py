import time
import requests
import zipfile
import io
import datetime
import pandas as pd

from decouple import config

from canvasapi import Canvas as CanvasApi

class Canvas:


    def __init__(self):
        self.canvas_admin = CanvasApi(config('api_url'), config('api_key')).get_account(1)


    def convert_term_id(self, jenzabar_term_id):
        """
        Convert Jenzabar term ID to corresponding Canvas term ID.
        """
        canvas_enrollment_terms = self.canvas_admin.get_enrollment_terms()

        for term in canvas_enrollment_terms:
            if term.sis_term_id == jenzabar_term_id:
                return term.id


    def create_provisioning_report(self, datasets, term_id):
        """ 
        Create a new provisioning report and wait for completion.
        """
        enrollment_term_dict = {"enrollment_term_id": term_id}
        parameters = {**datasets, **enrollment_term_dict}

        report = self.canvas_admin.create_report("provisioning_csv", parameters=parameters)
        while report.status != "complete":
            report = self.canvas_admin.get_report(report.report, report.id)
            time.sleep(3)

        report = self.canvas_admin.get_report(report.report, report.id)
        return report 


    def download_report(self, report, data_path):
        """
        Download a canvas report to a given data path.
        """
        request = requests.get(report.attachment["url"])
        zipf = zipfile.ZipFile(io.BytesIO(request.content))
        zipf.extractall(data_path / f"provisioning_report")


    def clean_report(self, dataset_names, data_path, term_id):
        for dataset_name in dataset_names.keys():
            clean_dataset = self._clean_dataset(data_path, dataset_name, term_id)
            dataset_path = (data_path / "provisioning_report_clean").mkdir(parents=True, exist_ok=True)
            clean_dataset.to_csv(data_path / "provisioning_report_clean" / f"{dataset_name}.csv", index=False)


    def _clean_dataset(self, data_path, dataset_name, term_id):
        dataset = pd.read_csv(data_path / "provisioning_report" / f'{dataset_name}.csv')
        add_term_id = False

        if dataset_name == "users":
            cols_to_keep = ["user_id", "canvas_user_id", "login_id"]
            cols_name_map = {"user_id": "id_num", "canvas_user_id": "canvas_user"}
            dataset = dataset[~dataset["user_id"].isna()]
            dataset = dataset[dataset["user_id"].str.isnumeric()]
            

        elif dataset_name == "courses":
            cols_to_keep = ["canvas_course_id", "course_id", "status"]
            cols_name_map = {"course_id": "crs_cde"}
            add_term_id = True

        elif dataset_name == "sections":
            cols_to_keep = ["course_id", "section_id", "name", "status",
                            "account_id", "canvas_section_id", "created_by_sis"]
            cols_name_map = {"course_id": "crs_cde"}
            add_term_id = True

        elif dataset_name == "enrollments":
            cols_to_keep = ["course_id", "user_id", "role", "section_id", 
                        "status", "canvas_enrollment_id", "canvas_section_id", "created_by_sis"]
            cols_name_map = {}
            add_term_id = True
            # dataset = dataset[~dataset["user_id"].str.startswith("CanvasStu")]

        dataset = dataset.loc[:, cols_to_keep]
        dataset = dataset.rename(cols_name_map, axis="columns")

        if "created_by_sis" in list(dataset):
            dataset["created_by_sis"] = dataset["created_by_sis"].replace({True: -1, False:0})

        if add_term_id:
            dataset["yr_cde"] = term_id[:2]
            dataset["trm_cde"] = term_id[2:4]

        dataset["load_date"] = datetime.date.today()
        
        
        return dataset


    def upload_all_updates(self, data_path):
        upload_order = [
            "faculty_users.csv",
            "student_users.csv",
            "courses.csv",
            "sections.csv",
            "enrollments.csv",
            "ctl_library_courses.csv",
            "ctl_library_sections.csv"
            ]

        reports = {}

        for dataset_name in upload_order:
            reports[dataset_name] = self._upload(data_path / "updates" / f'{dataset_name}')

        return reports

    def _upload(self, dataset_path):
        sis_import = self.canvas_admin.create_sis_import(str(dataset_path))

        while self.canvas_admin.get_sis_import(sis_import).progress != 100:
            time.sleep(2)

        sis_import_finished = self.canvas_admin.get_sis_import(sis_import)

        return sis_import_finished


    def save_report(self, reports, data_path):
        report_names = {
            "faculty_users.csv": "Account",
            "student_users.csv": "Account",
            "courses.csv": "Course",
            "sections.csv": "CourseSection",
            "enrollments.csv": "Enrollment",
            "ctl_library_courses.csv": "Course",
            "ctl_library_sections.csv": "CourseSection",
        }

        with open((data_path / "report.txt"), 'w') as report_file:
            for report_name in reports.keys():
                report_file.write(f'File Name: {report_name}\n')
                statistics = reports[report_name].data['statistics'][report_names[report_name]]
                report_file.write(f'Changes: {statistics} \n')
                try:
                    report_file.write(f'Warnings: {str(reports[report_name].processing_warnings)}\n')
                except AttributeError:
                    report_file.write("No warnings to report.\n")

                try:
                    report_file.write(f'Warnings: {str(reports[report_name].processing_errors)}\n')
                except AttributeError:
                    report_file.write("No errors to report.\n")

                report_file.write("############################\n")