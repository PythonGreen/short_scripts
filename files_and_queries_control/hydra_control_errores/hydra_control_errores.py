import json
import os
import psycopg2
import sys
import csv
import pandas as pd

# Get environment variable for source path
src_dir = os.getenv('BI_ETL_PY')

with open('{0}/configurations/db_config.json'.format(src_dir), 'r') as f:
        string = json.load(f)
        db_user = string['ods_maintenance']['user']
        db_host = string['ods_maintenance']['host']
        db_port = string['ods_maintenance']['port']
        db_pass = string['ods_maintenance']['password']
        db_name = string['ods_maintenance']['database']

conn = psycopg2.connect('dbname={0} user={1} password={2} host={3} port={4}'.format(db_name, db_user, db_pass, db_host, db_port))


with open('{0}/src/processes/control/hydra_control_errores/control_errores.sql'.format(src_dir), 'r') as sql_to_run:
	sql = sql_to_run.read()

data = pd.read_sql(sql, conn)
groupby_filename_qty_startime = data.groupby('filename').starttime.size()
qty_total_records = len(data)
qty_total_filenames = len(groupby_filename_qty_startime)

print("Errores detectados: {0},  files involucrados: {1}".format(qty_total_records, qty_total_filenames))

try:
	os.system('rm {0}/src/processes/control/hydra_control_errores/error_handling.csv'.format(src_dir))
except:
	print("There isn't any file to delete")	

try:
	os.system('rm *.gz')
except:
	print("There isn't any file to delete")

if qty_total_records > 0 and qty_total_records < 2000 :
	cur = conn.cursor()
	cur.execute(sql)	
	with open('{0}/src/processes/control/hydra_control_errores/error_handling.csv'.format(src_dir), 'w') as errors:
		writer = csv.writer(errors)
		header = [i[0] for i in cur.description]
		writer.writerow(header)
                for result in cur.fetchall():
                        writer.writerow(result)
	cur.close()

	if qty_total_filenames < 10:
		for a,i in groupby_filename_qty_startime.iteritems():
			os.system('aws s3 cp '+a+' ./')


	with open('{0}/src/processes/control/hydra_control_errores/logFile'.format(src_dir), 'w') as f:
		f.write(str(qty_total_records))

conn.close()

