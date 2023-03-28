import os
import uuid
from datetime import datetime
from multiprocessing import Process, Queue
from flask import Flask, jsonify, send_file, make_response
from services.report_generator_service import ReportGenerator
from multiprocessing.managers import BaseManager

app = Flask(__name__)


class ReportStatus:
    def __init__(self):
        self.status = {}

    def add_report(self, report_id, status):
        self.status[report_id] = {"STATUS": status, "REPORT_PATH": ""}

    def update_report(self, report_id, status, report_path=""):
        self.status[report_id]["STATUS"] = status
        self.status[report_id]["REPORT_PATH"] = report_path

    def get_report_status(self, report_id):
        if report_id not in self.status:
            return {"STATUS": "AN ERROR OCCURRED", "ERROR": "THE REPORT_ID  DOESN'T EXIST"}
        return self.status[report_id]


class MyManager(BaseManager):
    pass


MyManager.register('ReportStatus', ReportStatus)


@app.route("/trigger_report", methods=["GET"])
def trigger_report():
    report_id = str(uuid.uuid4())
    request_queue.put([report_id, datetime.now()])
    report_status_obj.add_report(report_id, "RUNNING")
    return jsonify({"REPORT_ID": report_id})


@app.route("/get_report/<id>", methods=["GET"])
def get_report(id):
    report_status_dict = report_status_obj.get_report_status(id)
    if report_status_dict["STATUS"] == "DONE":
        file_path = report_status_dict["REPORT_PATH"]
        response = make_response(send_file(file_path, as_attachment=True))
        response.headers["Content-Disposition"] = f"attachment; filename={file_path.split('/')[-1]}"
        return jsonify(report_status_dict), 200
    else:
        return jsonify(report_status_dict), 200


def generate_reports(queue, report_status):
    dir_path = os.path.dirname(os.path.realpath(__file__))
    print("directory_path", dir_path, os.path.join(dir_path, "/data_sources/status.csv"))
    report_generator = ReportGenerator(
        os.path.join(dir_path, "data_sources/status.csv"),
        os.path.join(dir_path, "data_sources/business_hours.csv"),
        os.path.join(dir_path, "data_sources/timezone.csv"),
        os.path.join(dir_path, "reports/"),
    )
    while True:
        if not queue.empty():
            report_id, triggered_time = queue.get()
            try:
                report_path = report_generator.get_detailed_report(report_id)
                report_status.update_report(report_id, "DONE", report_path)
            except Exception as e:
                report_status.update_report(report_id, "AN ERROR OCCURRED", str(e))


if __name__ == "__main__":
    with MyManager() as manager:
        request_queue = Queue()
        report_status_obj = manager.ReportStatus()
        consumer_process = Process(
            target=generate_reports,
            args=(request_queue, report_status_obj),
        )
        consumer_process.start()
        app.run(debug=True)
        consumer_process.join()
