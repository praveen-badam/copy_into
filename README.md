
* In V24 Dremio released "COPY INTO SQL" command to help customers to load csv/json data into Apache Iceberg tables.

  Important Links:
  
  https://docs.dremio.com/software/data-formats/copying-data-into-tables/
  
  https://docs.dremio.com/software/sql-reference/sql-commands/copy-into-table/
  


* This project is aimed to make customers to execute "COPY INTO" in their batch modules in a easy way.

* This project contanins 2 files. 

  - copy_into.py
    This script is designed to trigger "COPY INTO" command using SQL API.
  - copy_into_input_template.json
    This file is designed to pass all the parameters to copy_into.py script like iceberg table name, source data set, file_format, record_delimeter...etc
    
Guidenece to usage:

* Update dremio_host ip  in the below line in copy_ino.py script.
   
  sql_api = "http://<<dremio_host_ip>>:9047/api/v3/sql"
  
* Update token number at headers_sql in copy_into.py script

      headers_sql = {
        'Authorization': "_dremio<<dremio_token>>",
        'Content-Type': "application/json"
    }
    
* Substitute the patm file (along with its path at below code on copy_into.py script.

  f = open('/path/to/file/copy_into.json')
  
* Substitue dremio host ip at job_status_api in copy_into.py script.

   job_status_api = "http://<<dremio_host>>:9047/api/v3/job/%s" % (job_id)
   
* Edit input parm file with the below details:

target_table - Iceberg table to which data needs to be copied

source_data - File details from which data needs to be copied

file_format - The format of the file( CSV or JSON)

record_delimeter - Record Delimiter

timestamp_format - Time Stamp format of the input fields ( If there are any)

date_format - date format of the input fields (if there are any)

time_format - time format of the input fields (if there are any)

etc....

If we have requirement to load multiple tables (one after another), then update the above parametres for each in dictonary format and do a comma (,) seperation.

Ex:

    [
     {"target_table":"test.pbadamcopyinto.dataoutput.\"employee_demo1\"",
  
  "source_data":"'@test/pbadamcopyinto/datainput/employee_demo.csv'",
  
  "file_format":"'csv'",
  
  "record_delimeter":"'\n'",
  
  "timestamp_format":"'YYYY-MM-DD HH24:MI:SS.FFF'",
  
  "date_format":"'YYYY-MM-DD'",
  
  "time_format":"'HH24:MI:SS'"
    },

   {
  "target_table":"test.pbadamcopyinto.dataoutput.\"employee_demo2\"",
  
  "source_data":"'@test/pbadamcopyinto/datainput/employee_demo.csv'",
  
  "file_format":"'csv'",
  
  "record_delimeter":"'\n'",
  
  "timestamp_format":"'YYYY-MM-DD HH24:MI:SS.FFF'",
  
  "date_format":"'YYYY-MM-DD'",
  
  "time_format":"'HH24:MI:SS'"
    }
   ]

In case customer have  other attributes to pass (which are not covered here), they can pass those attributes in key-value format in the parm file and capture in copy_into.py script.

Command to run:

python3 copy_into.py


            


