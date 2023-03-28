from datetime import datetime, timezone


def is_timestamp_in_range(curr_time_stamp, start_timestamp, end_timestamp):
    curr_time_stamp = curr_time_stamp.replace(tzinfo=timezone.utc)
    start_timestamp = start_timestamp.replace(tzinfo=timezone.utc)
    end_timestamp = end_timestamp.replace(tzinfo=timezone.utc)
    return start_timestamp <= curr_time_stamp <= end_timestamp


def is_active(status):
    return "active" == status


def get_datetime_from_timestamp(timestamp):
    date_str = timestamp.split(" ")
    time_str = date_str[1].split(".")
    return datetime.strptime(date_str[0] + " " + time_str[0], "%Y-%m-%d %H:%M:%S")
