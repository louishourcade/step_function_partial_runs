import boto3
import os
import pandas as pd
import requests
import json
from io import StringIO
from datetime import datetime

""" This function generates a csv file with velib data """

# Frequency in second. How often do you want to query the data
freq = 1
# How many time do you want to query the data ?
nb_iteration = 1
# File name
dateTimeObj = datetime.now()
timestamp = f"{dateTimeObj.year}-{dateTimeObj.month}-{dateTimeObj.day}-{dateTimeObj.hour}-{dateTimeObj.minute}-{dateTimeObj.second}"
file_name = f"velib-data-raw/{timestamp}/velib-availability.csv"
# bucket="step-function-poc-949035406805"
bucket=os.environ.get("S3_BUCKET_NAME")

def handler(event,context):
    print(f"Starting Lambda function")
    nbrows = 2000
    url = "https://opendata.paris.fr/api/records/1.0/search/?dataset=velib-disponibilite-en-temps-reel&q=&rows=" + str(nbrows)
    resp = requests.get(url)

    if resp.status_code != 200:
        print("Error in querying velib data")
    else:
        records = json.loads(resp.content)["records"]
        columns = [
            'record_timestamp',
            'stationcode',
            'is_renting',
            'is_returning',
            'is_installed',
            'capacity',
            'numbikesavailable',
            'numdocksavailable',
            'name',
            'mechanical',
            'ebike',
            'nom_arrondissement_communes',
            'coordonnees_geo'
        ]
        dff = pd.DataFrame(columns=columns)

        for rec in records:
            dff.loc[len(dff)] = [
                rec.get("record_timestamp"),
                rec.get("fields").get("stationcode"),
                rec.get("fields").get("is_renting"),
                rec.get("fields").get("is_returning"),
                rec.get("fields").get("is_installed"),
                rec.get("fields").get("capacity"),
                rec.get("fields").get("numbikesavailable"),
                rec.get("fields").get("numdocksavailable"),
                rec.get("fields").get("name"),
                rec.get("fields").get("mechanical"),
                rec.get("fields").get("ebike"),
                rec.get("fields").get("nom_arrondissement_communes"),
                rec.get("fields").get("coordonnees_geo")
            ]

    csv_buffer = StringIO()
    dff.to_csv(csv_buffer, index=False)
    s3_resource = boto3.resource('s3')
    s3_resource.Object(bucket, file_name).put(Body=csv_buffer.getvalue())

    return {
        "Status": "SUCCESS",
        "timestamp": timestamp
    }