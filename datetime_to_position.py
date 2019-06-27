import sqlite3
import os


class DatetimeToPosition(object):
    def __init__(self):
        environment_variable = "DATETIME_POSITIONS_SQLITE3_PATH"
        if environment_variable not in os.environ:
            print("Define", environment_variable, "environment variable to the file with the positions")
        database_path = os.environ[environment_variable]

        uri = "file:{}?mode=ro".format(database_path)
        conn = sqlite3.connect(uri, uri=True)

        self.sqlite3_cur = conn.cursor()

        self.sqlite3_cur.execute("PRAGMA case_sensitive_like = 1")

    def datetime_datetime_to_position(self, datetime_datetime):
        return self.datetime_text_to_position(datetime_datetime.strftime("%Y-%m-%dT%H:%M:%S"))

    def datetime_text_to_position(self, datetime_text):
        approximation = datetime_text + "%"
        self.sqlite3_cur.execute('SELECT latitude,longitude FROM gps where date_time like ?', (approximation, ))

        result = self.sqlite3_cur.fetchone()

        if result is None:
            return None

        return float(result[0]), float(result[1])
