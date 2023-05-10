import os
import pandas as pd
import json
import pymysql
import datetime
import boto3
import socket
import pyarrow.parquet as pq
from requests import Request, Session
from io import StringIO, BytesIO
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
from dotenv import load_dotenv



#make a request to the Faker API to get fake user data
response = requests.get('https://fakerapi.it/api/v1/users?_quantity=10')

#convert the extraction data to JSON format

data = response.json()
#if api comunication is sucesfull = prints '200' on the terminal
print(response)

#create an empty list to store the transformed data

transformed_data = []

#loop throught each record from json file and extract desired fields

for record in data['data']:
    transformed_record = {
        'Name': record['firstname'] + ' ' + record['lastname'],
        'Email': record['email'],
        'IP': record['ip']
    }
    transformed_data.append(transformed_record)

#API debugging
#print(transformed_data[0:4])

# #Convert the transformed data to a pandas DataFrame:
df_api = pd.DataFrame(transformed_data)


# #saving it as a local file:
#storing as a csv file (to load inside a db for example or AWS S3)
df_api.to_csv('./my_api_data.csv', index=False, mode='w+')
#sorting as a .parquet (to save space -> optimal in cloud storage, save resources)
df_api.to_parquet('./my_api_data.parquet', index=False)



######## conections ######
#IP check:
hostname = socket.gethostname()
IPAddr = socket.gethostbyname(hostname)
print("Your Computer Name is:"+hostname)
print("Your Computer IP Address is:"+IPAddr)

#load dataframe to AWS RDS
load_dotenv()
host = 'database-aws.csyqer7pwgvm.us-east-1.rds.amazonaws.com'
port = 3306
#Note: This is VERY sensitive data and SHOULD NOT be exposed in you code, the example below is just to ilustrate purposes ONLY
user = 'admin'
password = 'password123'

# Create connection to AWS database

connection = pymysql.connect(host = host,
                             port = port,
                             user = user,
                             passwd = password)
 
cursor = connection.cursor()

#create data base in AWS RDS:

query = '''create database fakerAPI'''
cursor.execute(query)
cursor.connection.commit()

# Connect to data base

query = '''use database FakerAPI'''
cursor.execute(query)

#display databases:
# Connect to data base

query = '''use FakerAPI'''
cursor.execute(query)

# Show existing databases

query = '''show databases'''
cursor.execute(query)
cursor.fetchall()



# Connect to data base

query = '''use crypto_data_base'''
cursor.execute(query)

# Create table

query = '''create table FakerAPI ( 
              Name varchar(50), 
              Email varchar(50), 
              IP varchar(50)
              )
              '''

cursor.execute(query)


#isnert data from dataframe:

# Connect to data base

query = '''use crypto_data_base'''
cursor.execute(query)


# Insert data from df_api

for index, row in df_api.head(100).iterrows():
    print(f'\nindex = {index} / {len(df_api)}')

    query = '''INSERT INTO FakerAPI

               (Name, Email, Ip) 

               values(%s, %s, %s)'''

    Name = str(row['Name']) if row['Name'] is not None else None 
    Email = str(row['Email']) if row['Email'] is not None else None
    Ip = str(row['Ip']) if row['Ip'] is not None else None
  
    values = Name, Email, Ip

    cursor.execute(query, values)

connection.commit()



# Connect to data base

query = '''use FakerAPI'''
cursor.execute(query)

# Select all data from AWS RDS

query = '''select * from FakerAPi'''
cursor.execute(query)
results = cursor.fetchall()
  
# Printing all records or rows from the table.
# It returns a result set. 

for all in results[0:10]:
  print(all)



#####

#load data to S3


# AWS S3 credentials
# This is VERY sensitive data and should NOT be stored inside de code

aws_access_key_id = 'AWS_ACCESS_KEY_ID'
aws_secret_access_key = 'AWS_SECRET_ACCESS_KEY'

# Create connection to AWS S3

s3 = boto3.client('s3', 
                  aws_access_key_id = aws_access_key_id,
                  aws_secret_access_key = aws_secret_access_key)

# Get the list of all buckets

s3.list_buckets()['Buckets']

bucket_name = 'bucket-upload-test'

# List all objects in the folder

response = s3.list_objects_v2(Bucket = bucket_name)

for obj in response['Contents']:
    print(obj['Key'])


bucket_name = 'bucket-for-tests-aws'
folder = 'json/'
file_name = 'data.json'
file_full_path = folder + file_name

# Convert the list of dictionaries to a csv string

bucket_name = 'bucket-for-tests-aws'
folder = 'csv/'
file_name = 'data.csv'
file_full_path = folder + file_name

# Convert the DataFrame to a CSV string

csv_buffer = StringIO()
df_api.to_csv(csv_buffer, index=False)
csv_string = csv_buffer.getvalue()

# Upload the file to S3

s3.put_object(Body = csv_string, 
              Bucket = bucket_name, 
              Key = file_full_path)