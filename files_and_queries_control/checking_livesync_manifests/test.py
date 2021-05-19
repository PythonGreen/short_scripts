import boto3
import datetime
from datetime import timedelta
from dateutil.parser import parse
import os
import json
import s3fs
import pandas as pd
#import click
import sys

def connect_to_bucket(role_param, kinesis_role_param=''):

    # create an STS client object that represents a live connection to the 
    # STS service
    client = boto3.client('sts')

    # Call the assume_role method of the STSConnection object and pass the role
    # ARN and a role session name.
    role_object = client.assume_role(
        RoleArn=role_param,
        RoleSessionName=str("default"),
        DurationSeconds=3600)

    # From the response that contains the assumed role, get the temporary 
    # credentials that can be used to make subsequent API calls
    credentials = role_object['Credentials']

    
    if kinesis_role_param != '':
        firewall_client = boto3.client(
            'sts',
            aws_access_key_id=credentials['AccessKeyId'],
            aws_secret_access_key=credentials['SecretAccessKey'],
            aws_session_token=credentials['SessionToken'], )

        optimizely_role_object = firewall_client.assume_role(
            RoleArn=kinesis_role_param,
            RoleSessionName=str("default"),
            DurationSeconds=3600)

        credentials=optimizely_role_object['Credentials']

    # Use the temporary credentials that AssumeRole returns to make a 
    # connection to Amazon S3  
    s3_resource=boto3.resource(
        's3',
        aws_access_key_id=credentials['AccessKeyId'],
        aws_secret_access_key=credentials['SecretAccessKey'],
        aws_session_token=credentials['SessionToken'],
    )
    
    return s3_resource
# ************* END FUNCTION: connect_to_bucket


def get_filenames_reservoir(reservoir_resource, table_reservoir_path, path_day):    
#--------------------------------------------------------------------------------------------------------
#----------------------------- # Get filenames list in the latam-bi S3 reservoir ------------------------
#--------------------------------------------------------------------------------------------------------
    list_files_latambi_reservoir = []
    for bucket in reservoir_resource.buckets.all():
        if bucket.name == 'olxgroup-reservoir-latam-bi':
            for obj in bucket.objects.filter(Prefix='{}{}/'.format(table_reservoir_path,path_day)):
                list_files_latambi_reservoir.append(obj.key)
                #print(obj.key, '--------------')
    return list_files_latambi_reservoir
# ************* END FUNCTION: get_filenames_reservoir

def get_filenames_bucket(bucket_resource, table_bucket_path, date_from):
    import s3fs
#--------------------------------------------------------------------------------------------------------
#----------------------------- # Get filenames list in the BI S3 Bucket ------------------------
#--------------------------------------------------------------------------------------------------------        
    list_files_bi_bucket = []
    for bucket in bucket_resource.buckets.all():
        if bucket.name == 'olx-latam-bi':
            for obj in bucket.objects.filter(Prefix=table_bucket_path):
                subresource = obj.Object()
                if (obj.last_modified.date() >= date_from ):
                    
                    ## Version Code downloading the file to ubuntu server
                    #subresource.download_file('/home/ubuntu/transformaciones/input/temp/PAD/tmpfiles/'+obj.key[-24:])
                    #with open('/home/ubuntu/transformaciones/input/temp/PAD/tmpfiles/'+obj.key[-24:]) as json_file:  
                    #    data = json.load(json_file)
                    #    for entry in data['entries']:
                    #        list_files_bi_bucket.append(entry['url'][33:])
                    
                    ## Version Code reading the file directly from S3 bucket with pandas
                    df = pd.read_json('s3://{}/{}'.format(bucket.name,obj.key))
                    for entry in df['entries']:
                        #print(entry['url'][33:])
                       list_files_bi_bucket.append(entry['url'][33:])    
    
    return list_files_bi_bucket
# ************* END FUNCTION: get_filenames_bucket


#@click.command()
#@click.option('--configfile', prompt='ConfigFile', help='Config file with the tables to check')
def get_tables_to_check(configfile):

    #print("FILENAME: ->>",configfile)
    with open (configfile) as f:
        csv_file = f.readlines()

    csv_file_list = []
    for a in csv_file[1:]:
        csv_file_list.append(a.split(';'))

    #print("CONTENT CONFIGFILE",csv_file_list) 
    return csv_file_list

def get_missing_files(reservoir_resource, bucket_resource, table_reservoir_path, table_bucket_path, date_from_date, date_to_date):
    # Get S3 files from 
    files_bi_reservoir = []
    dif = (date_to_date-date_from_date).days
    for addday in range(0,dif+1):
        day = date_from_date + timedelta(days=addday)
        path_day = str(day).replace('-', '/')
        print("Working with {}{}".format(table_reservoir_path, path_day) )
        files_bi_reservoir = files_bi_reservoir + get_filenames_reservoir(reservoir_resource, table_reservoir_path, path_day)

    #for a in files_bi_reservoir: print(a)

    files_bi_bucket = []
    files_bi_bucket = get_filenames_bucket(bucket_resource, table_bucket_path, date_from_date)

    #for a in files_bi_bucket: print("ALGO POR AQUI ->>",a)

    missing_files = []
    for a in files_bi_reservoir:
        if a not in files_bi_bucket:
            missing_files.append(a)

    return missing_files
    
def main():

    date_from =  sys.argv[-4]
    date_to =  sys.argv[-3]
    configfilename = sys.argv[-2]
    workspace_path = sys.argv[-1]

    # Clean the temporary working folder
    #try:
    #    os.system('rm {}/missing*'.format(workspace_path))
    #except:
    #    print("Missing file does not exist")

    if date_from == '%' and date_to == '%':
        date_from = str(datetime.date.today() - timedelta(days=4))
        date_to = str(datetime.date.today() - timedelta(days=1))


    date_from_path = date_from.replace('-', '/')
    date_from_date = parse(date_from).date()
    date_to_path = date_to.replace('-', '/') #*
    date_to_date = parse(date_to).date() #*
    print(date_from)
    print(date_to)
    print(date_from_path)
    print(date_from_date)
    print(date_to_path)
    print(date_to_date)
          

if __name__ == "__main__":
        main()
