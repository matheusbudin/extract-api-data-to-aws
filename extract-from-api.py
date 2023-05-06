import os
import requests
import pandas as pd
import json


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
#sotring as a .parquet (to save space -> optimal in cloud storage, save resources)
df_api.to_parquet('./my_api_data.parquet', index=False)

