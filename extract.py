import gzip
import json
import os
from datetime import datetime

import psycopg2
from psycopg2 import Error
from psycopg2.extras import execute_values


LOG_FOLDER = "logs"
LOG_FORMAT = "text"
TABLE_NAME = "tracking_logs"
# START_TIME = datetime(2022, 3, 2)

psql = {
            "user": "tmshszilbcculx",
            "password": "ee95204861668778837467651b39614a5529edd88412c1fe0a8b9a21419bf437",
            "host": "ec2-54-246-185-161.eu-west-1.compute.amazonaws.com",
            "port": "5432",
            "database": "dd83caln9k71ig"
        }


def create_table():
    try:
        psql_connection = psycopg2.connect(
            user = psql['user'],
            password = psql['password'],
            host = psql['host'],
            port = psql['port'],
            database = psql['database']
        )

        cursor = psql_connection.cursor()
        # SQL query to create a new table
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            log_id INT PRIMARY KEY NOT NULL,
            raw_log TEXT UNIQUE NOT NULL,
            user_id VARCHAR(10) NULL,
            org_id TEXT NULL,
            course_id TEXT NULL,
            event_type TEXT NOT NULL,
            event_source TEXT NOT NULL,
            ip VARCHAR(15) NOT NULL,
            date DATE NOT NULL,
            time TIME NOT NULL,
            referer TEXT NULL
            )
        """
        # Execute a command: this creates a new table
        cursor.execute(create_table_query)
        psql_connection.commit()
        print("Table created successfully in PostgreSQL")

    except (Exception, Error) as error:
        print("Error while connecting to PostgreSQL", error)
    finally:
        if psql_connection:
            cursor.close()
            psql_connection.close()
            print("PostgreSQL sql_connection is closed")


def logfiles_to_json():
    logs = []

    ''' Loop through the logs directory and parse each log file '''
    if LOG_FORMAT == "text":
        for file in os.listdir(LOG_FOLDER):
            # if datetime.utcfromtimestamp(os.path.getmtime(f'{LOG_FOLDER}/{file}')) < START_TIME: continue
            with open(os.path.join(LOG_FOLDER, file), "rb") as f:
                for line in f:
                    logs.append(json.loads(line.decode('utf-8')))

    return logs


def get_required_events(logs):
    events_startswith = ['/courses/', '/user_api/v1/account/registration']
    required_events = set()

    for log in logs:
        if(log['event_type'].find('/') == -1):
            required_events.add(log['event_type'])
        elif(log['event_type'].startswith(tuple(events_startswith))):
            required_events.add(log['event_type'])

    return required_events


def count_rows():
    try:
        psql_connection = psycopg2.connect(
            user = psql['user'],
            password = psql['password'],
            host = psql['host'],
            port = psql['port'],
            database = psql['database']
        )

        cursor = psql_connection.cursor()
        # SQL query to count rows
        cursor.execute(f"""SELECT COUNT(*) FROM {TABLE_NAME}""")
        result = cursor.fetchone()
        log_id = result[0]
        return log_id

    except (Exception, Error) as error:
        print("Error while connecting to PostgreSQL", error)
    finally:
        if psql_connection:
            cursor.close()
            psql_connection.close()
            print("PostgreSQL sql_connection is closed")


def parse_time(line):
    date, time = line[:10], line[11:19]
    return f"{date}", f"{time}"

def parse_log(line):
    raw_log = f"{line}"
    user_id = f"{line['context']['user_id']}" if line['context']['user_id'] != None else "anonymous"
    org_id = f"{line['context']['org_id']}"
    course_id = f"{line['context']['course_id']}"
    event_type = f"{line['event_type']}"
    event_source = f"{line['event_source']}"
    ip = f"{line['ip']}"
    date, time = parse_time(line['time'])
    referer = f"{line['referer']}"

    return [raw_log, user_id, org_id, course_id, event_type, event_source, ip, date, time, referer]


def store_data(sql_data):
    try:
        psql_connection = psycopg2.connect(
            user = psql['user'],
            password = psql['password'],
            host = psql['host'],
            port = psql['port'],
            database = psql['database']
        )

        cursor = psql_connection.cursor()
        # SQL query to count rows
        insert_query = f"""INSERT INTO {TABLE_NAME} (log_id, raw_log, user_id, org_id, course_id, event_type, event_source, ip, date, time, referer) 
        VALUES %s"""
        execute_values(cursor, insert_query, sql_data)
        psql_connection.commit()
        print("Stored data successfully")

    except (Exception, Error) as error:
        print("Error:", error)
    finally:
        if psql_connection:
            cursor.close()
            psql_connection.close()
            print("PostgreSQL sql_connection is closed")




# DRIVER FUNCTION
if __name__ == "__main__":
    create_table()
    try:
        logs = logfiles_to_json()
        required_events = get_required_events(logs)
        # print(logs)
        tracking_logs = []
        event_data = []

        log_id = count_rows() + 1

        for line in logs:
            if line['event_type'] in required_events:
                parsed_log = [log_id] + parse_log(line)
                tracking_logs.append(tuple(parsed_log))
                log_id += 1

        store_data(tracking_logs)

    except Exception as e:
        print(e)
