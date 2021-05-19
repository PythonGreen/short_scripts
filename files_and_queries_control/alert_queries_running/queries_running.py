#!/usr/bin/env python3


import json
import psycopg2
import csv

def load_file(filename):
    with open(filename, 'r') as f:
        if filename[filename.find('.')+1:] == 'sql':
            content = f.read()
        if filename[filename.find('.')+1:] == 'json':
            content = json.load(f)

    return content


def execute_query():

    connection = load_file(filename="/opt/latambi/latam-bi-etl/configurations/db_config.json")

    user = connection['ods_maintenance']['user']
    password = connection['ods_maintenance']['password']
    host = connection['ods_maintenance']['host']
    port = connection['ods_maintenance']['port']
    dbname = connection['ods_maintenance']['database']
    #print(user, host, password, port, dbname)


    conn = psycopg2.connect(dbname=dbname, user=user, host=host, password=password, port=port)
    conn.set_session(autocommit=True)



    query_to_run = load_file(filename="query_to_run.sql")


    cur = conn.cursor()
    cur.execute(query_to_run)
    records = cur.fetchall()
    header = [i[0] for i in cur.description]

    print("Queries currently running for more than 1 hour: ",cur.rowcount )

    
    if cur.rowcount > 0:
        #header = ['cancel_query', 'pid', 'user_name', 'starttime_local', 'query', 'seconds_running']
        with open("queries_running.csv", 'w') as csv_file:
            writer = csv.writer(csv_file )
            writer.writerow(header)
            for i in records:
                writer.writerow(i)

    cur.close()


if __name__ == "__main__":
    execute_query()
