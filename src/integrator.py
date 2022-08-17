from pathlib import Path
from datetime import datetime

from src.jenzabar import Jenzabar
from src.canvas import Canvas

import requests

class Integrator:


    def __init__(self, term="current"):
        self.jenzabar = Jenzabar()
        self.canvas = Canvas()
        self.term_id = self._get_term_id(term)
        self.datasets = {"users": True, "courses": True, "sections": True, "enrollments": True}
        self.data_path = Path("data/" + str(datetime.now().strftime("%Y-%m-%d_%H-%M")) + "_" + self.term_id["jenzabar"])


    def _get_term_id(self, term="current"):
        term_ids = {"jenzabar": "", "canvas": ""}
        jenzabar_term_id = self.jenzabar.get_current_term_id()

        if term == "current":
            term_ids["jenzabar"] = jenzabar_term_id
            term_ids["canvas"] = self.canvas.convert_term_id(term_ids["jenzabar"])
        elif term == "next":
            year = jenzabar_term_id[:2]
            semester = jenzabar_term_id[2:]
            if semester == "2S":
                year = str(int(year)+1)
                semester = "1S"
            elif semester == "1S":
                semester = "2S"
            term_ids["jenzabar"] = year + semester
            term_ids["canvas"] = self.canvas.convert_term_id(term_ids["jenzabar"])
            
        return term_ids

    def update_mirror_tables(self):
        # jenzabar_term_id = self.jenzabar.get_current_term_id()
        # canvas_term_id = self.canvas.convert_term_id(jenzabar_term_id)
        print("Creating Provisioning Report from Canvas...")
        report = self.canvas.create_provisioning_report(self.datasets, self.term_id["canvas"])
        print("Downloading Report...")
        self.canvas.download_report(report, self.data_path)
        print("Cleaning Report...")
        self.canvas.clean_report(self.datasets, self.data_path, self.term_id["jenzabar"])
        print("Uploading Report to Canvas mirror tables in SQL...")
        self.jenzabar.upload_report_to_sql(self.data_path, self.datasets)
        
    
    def update_canvas(self):
        print("Comparing Mirror tables with SQL's data...")
        self.jenzabar.download_all_updates(self.data_path, self.term_id["jenzabar"])
        print("Uploading updates to Canvas through SIS import...")
        reports = self.canvas.upload_all_updates(self.data_path)
        self.canvas.save_report(reports, self.data_path)
        print("=================================================")
        print(f'Operation finished succesfully. Report saved in {self.data_path}')