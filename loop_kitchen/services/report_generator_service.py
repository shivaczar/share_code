import csv
import datetime
import logging
import os
import pandas as pd
from pytz import timezone


from util.util import is_timestamp_in_range, is_active, get_datetime_from_timestamp


class ReportGenerator:
    def __init__(self, store_status_path, business_hours_path, timezone_map_path, report_path):
        self.report_path = report_path

        with open(store_status_path) as store_status_file:
            store_status_list = csv.reader(store_status_file)
            self.store_status_list_map = {}
            next(store_status_list)  # skip header row
            for store_entry in store_status_list:
                store_id = store_entry[0]
                datetime_obj = get_datetime_from_timestamp(store_entry[2])
                self.store_status_list_map.setdefault(store_id, []).append(
                    [store_entry[1], datetime_obj]
                )
            logging.info(f"Loaded store status for {len(self.store_status_list_map)} stores")

        with open(timezone_map_path) as timezone_file:
            timezone_list = csv.reader(timezone_file)
            self.timezone_map = {}
            next(timezone_list)  # skip header row
            for row in timezone_list:
                self.timezone_map[row[0]] = row[1]
            logging.info(f"Loaded timezone map for {len(self.timezone_map)} stores")

        with open(business_hours_path) as business_hours_file:
            business_hours_list = csv.reader(business_hours_file)
            self.business_hours_map = {}
            next(business_hours_list)  # skip header row
            for row in business_hours_list:
                self.business_hours_map.setdefault(row[0], []).append(row[1:])
            logging.info(f"Loaded business hours for {len(self.business_hours_map)} stores")

    def get_detailed_report(self, report_id):
        curr_time = datetime.datetime.strptime("2023-01-24 13:06:07", "%Y-%m-%d %H:%M:%S")
        all_report = []
        for store_id in self.store_status_list_map.keys():
            last_hour = curr_time - datetime.timedelta(hours=1)
            last_day = curr_time - datetime.timedelta(days=1)
            last_week = curr_time - datetime.timedelta(weeks=1)
            uptime_last_hour, downtime_last_hour = self.generate_report(
                store_id, last_hour, curr_time
            )
            uptime_last_day, downtime_last_day = self.generate_report(
                store_id, last_day, curr_time
            )
            uptime_last_week, downtime_last_week = self.generate_report(
                store_id, last_week, curr_time
            )
            report = {
                "store_id": store_id,
                "uptime_last_hour": uptime_last_hour,
                "uptime_last_day": uptime_last_day,
                "uptime_last_week": uptime_last_week,
                "downtime_last_hour": downtime_last_hour,
                "downtime_last_day": downtime_last_day,
                "downtime_last_week": downtime_last_week,
            }
            all_report.append(report)

        now = datetime.datetime.now()
        date_time_for_csv = now.strftime("%Y-%m-%d_%H-%M-%S")
        dump_path = os.path.join(self.report_path, "report_" + report_id[:5] + date_time_for_csv + ".csv")
        df = pd.DataFrame(all_report)
        df.to_csv(dump_path)
        return dump_path

    def generate_report(self, store_id, start_timestamp, end_timestamp):
        active_count = 0
        inactive_count = 0
        for store_status in self.store_status_list_map.get(store_id):
            curr_time_stamp = store_status[1]
            if is_timestamp_in_range(curr_time_stamp, start_timestamp, end_timestamp):
                if is_active(store_status[0]):
                    active_count += 1
                else:
                    inactive_count += 1

        business_hours = self.business_hours_map.get(store_id, [])
        if not business_hours:
            return [active_count, inactive_count]

        business_hours_count = 0
        for business in business_hours:
            start_time = datetime.datetime.strptime(business[1], "%H:%M:%S")
            end_time = datetime.datetime.strptime(business[2], "%H:%M:%S")

            timezone_val = self.timezone_map.get(store_id, "America/Chicago")
            start_time_local = timezone(timezone_val).localize(start_time)
            end_time_local = timezone(timezone_val).localize(end_time)

            start_time_utc = start_time_local.astimezone(timezone("UTC"))
            end_time_utc = end_time_local.astimezone(timezone("UTC"))

            if start_time_utc > end_time_utc:
                end_time_utc = end_time_utc + datetime.timedelta(days=1)

            if is_timestamp_in_range(curr_time_stamp, start_time_utc, end_time_utc):
                business_hours_count += 1

        if business_hours_count > 0:
            return [active_count, inactive_count]
        else:
            return [0, active_count + inactive_count]


# if __name__ == "__main__":
#     print("started")
#     dir_path = os.path.dirname(os.path.realpath(__file__))
#     print("directory_path", dir_path, os.path.join(dir_path, "/data_sources/status.csv"))
#     report_generator = ReportGenerator(
#         os.path.join(dir_path, "data_sources/status.csv"),
#         os.path.join(dir_path, "data_sources/business_hours.csv"),
#         os.path.join(dir_path, "data_sources/timezone.csv"),
#         os.path.join(dir_path, "reports/"),
#     )
#     report_path = report_generator.get_detailed_report("report_id")
#     print(report_path)
