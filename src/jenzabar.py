from decouple import config
from pathlib import Path

import sqlalchemy as db
import pandas as pd
import urllib.parse

import pyodbc


class Jenzabar:


    def __init__(self):
        password = urllib.parse.quote_plus(config('sis_password'))
        self.sis_engine = db.create_engine(f"mssql+pyodbc://{config('sis_username')}:{password}@{config('sis_server')}/{config('sis_database')}?driver=SQL+Server+Native+Client+10.0")


    def get_current_term_id(self):
        """
        Get the current working Jenzabar's term id from REG_CONFIG.
        """
        with self.sis_engine.connect() as conn:
            reg_config = pd.read_sql("REG_CONFIG", conn)

        term_id = reg_config['CUR_YR_DFLT'][0].strip() + reg_config['CUR_TRM_DFLT'][0].strip()

        return term_id


    def upload_report_to_sql(self, datapath, dataset_names):
        """
        Upload a cleaned canvas report to Jenzabar's SQL server.
        """
        allowed_targets = ["rpc_RE_Canvas_Users", "rpc_RE_Canvas_Courses", 
                           "rpc_RE_Canvas_Sections", "rpc_RE_Canvas_Enrollments"]

        for dataset_name in dataset_names:
            dataset = pd.read_csv(datapath / "provisioning_report_clean" / f'{dataset_name}.csv')   
            target_table = f'rpc_RE_Canvas_{dataset_name.capitalize()}'

            if target_table not in allowed_targets:
                raise NameError("Chosen target is not in the scope of the project.")

            with self.sis_engine.connect() as conn:
                conn.execute(f'DELETE FROM {target_table}')
                dataset.to_sql(target_table, conn, if_exists='append', index=False, chunksize=100, method="multi")


    def download_all_updates(self, data_path, term_id):
        update_queries = {
            "faculty_users.csv": "MissingFacultyUsers.sql",
            "student_users.csv": "MissingStudentUsers.sql",
            "courses.csv": "MissingCourses.sql",
            "sections.csv": "MissingSections.sql",
            "enrollments.csv": "DailyEnrollment.sql",
            "ctl_library_courses.csv": "CtlLibraryCourses.sql",
            "ctl_library_sections.csv": "CtlLibrarySections.sql"
        }

        updates_path = (data_path / "updates")
        updates_path.mkdir(parents=True, exist_ok=True)

        for file_name in update_queries.keys():
            update = self._get_update(update_queries[file_name], term_id)
            update.to_csv(updates_path / f'{file_name}', index=False)


    def _get_update(self, querie_name, term_id):

        with open(Path(f'src/queries/{querie_name}')) as querie_file:
            querie = querie_file.read()

            with self.sis_engine.connect() as conn:
                update = pd.read_sql(querie, conn, params=[term_id[:2], term_id[2:]])

        if "user_id" in list(update):
            update["user_id"] = update["user_id"].astype("Int64")

        return update

