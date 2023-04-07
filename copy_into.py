import json
import time

import requests

if __name__ == '__main__':

    #sql_api = "https://api.dremio.cloud/v0/projects/f077cdb6-6b6d-4c63-b6bf-357f3e4673d6/sql"
    sql_api="http://ac0eacaf182f14c37b9f4581dbfa1c65-1827956046.ap-south-1.elb.amazonaws.com:9047/api/v3/sql"

    headers_sql = {
        #'Authorization': "Bearer bB91sotMTe6wnc5AlFXeMll5Y/eTMeRcr5K5f1hcyCU02nNaeN3a/K0njHKF/A==",
        'Authorization': "_dremiovmfcs5geqpgssimed93gajfbec",
        'Content-Type': "application/json"
    }

    f = open('copy_into_V3.json')
    print("data:", f)
    print(type(f))

    file_data = json.load(f)

    for data in file_data:
        print(data)
        print(type(data))

        target_table = data["target_table"]
        print(target_table)
        source_data = data["source_data"]
        print(source_data)
        file_format = data["file_format"]
        print(file_format)
        record_delimeter = data["record_delimeter"]
        print(record_delimeter)
        timestamp_format = data["timestamp_format"]
        print(timestamp_format)
        date_format = data["date_format"]
        print(date_format)
        time_format = data["time_format"]
        print(time_format)

        sql_payload = {
            "sql": "COPY INTO %s "
                   "from %s "
                   "file_format %s "
                   "(RECORD_DELIMITER  %s, "
                   "TIMESTAMP_FORMAT %s, "
                   "DATE_FORMAT %s, "
                   "TIME_FORMAT %s)" % (
                   target_table, source_data, file_format, record_delimeter, timestamp_format, date_format, time_format)
        }

        print("sql_payload:", sql_payload)

        data_payload = json.dumps(sql_payload)
        print("data_payload:", data_payload)

        response_sql = requests.request("POST", sql_api, data=data_payload, headers=headers_sql)

        response_sql_text = response_sql.text

        job_id = json.loads(response_sql_text)["id"]
        print("job_id:", job_id)

        job_status_api = "http://ac0eacaf182f14c37b9f4581dbfa1c65-1827956046.ap-south-1.elb.amazonaws.com:9047/api/v3/job/%s" % (
            job_id)

        while True:
            try:
                # extracting Job Status
                job_status_call = requests.request("GET", job_status_api, headers=headers_sql)

                job_status = json.loads(job_status_call.text)["jobState"]

                if job_status == 'COMPLETED':
                    print("job_completed")
                    break
                if job_status == 'FAILED':
                    print("job got failed")
                    break
                else:
                    print("job not completed")
                    time.sleep(30)
                    continue
            except Exception as e:
                print("something went wrong")
                break
                
