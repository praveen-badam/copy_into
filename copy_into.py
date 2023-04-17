import json
import time

import requests

def main():

    sql_api = "http://<<dremio_host>>:9047/api/v3/sql"

    headers_sql = {
        'Authorization': "_dremio<<token>>",
        'Content-Type': "application/json"
    }
    
# Opening parm file

    f = open('copy_into_V4.json')

    file_data = json.load(f)

    for data in file_data:

# Extracting all parameters
        
        target_table = data["target_table"]
        source_data = data["source_data"]
        file_format = data["file_format"]
        record_delimeter = data["record_delimeter"]
        timestamp_format = data["timestamp_format"]
        date_format = data["date_format"]
        time_format = data["time_format"]

# Constructing SQL Payload

        sql_payload = {
            "sql": "COPY INTO %s "
                   "from %s "
                   "file_format %s "
                   "(RECORD_DELIMITER  %s, "
                   "TIMESTAMP_FORMAT %s, "
                   "DATE_FORMAT %s, "
                   "TIME_FORMAT %s)" % (
                       target_table, source_data, file_format, record_delimeter, timestamp_format, date_format,
                       time_format)
        }


        data_payload = json.dumps(sql_payload)
        
# Making SQL Api

        response_sql = requests.request("POST", sql_api, data=data_payload, headers=headers_sql)

        response_sql_text = response_sql.text

        job_id = json.loads(response_sql_text)["id"]
        print("job_id:", job_id)

        job_status_api = "http://<<dremio_host>>:9047/api/v3/job/%s" % (
            job_id)

        while True:
            try:
                # extracting Job Status
                job_status_call = requests.request("GET", job_status_api, headers=headers_sql)

                job_status = json.loads(job_status_call.text)["jobState"]

                if job_status == 'COMPLETED':
                    print("job_completed for %s" %target_table)
                    break
                if job_status == 'FAILED':
                    print("job_failed for %s" %target_table)
                    break
                else:
                    print("job  is running for %s" %target_table)
                    time.sleep(30)
                    continue
            except Exception as e:
                print("something went wrong, please check the logs")
                break


if __name__ == '__main__':
    main()
