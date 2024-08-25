import csv
import boto3
import pandas as pd
from elasticsearch.helpers import bulk
from common_codes import mongo_connect, elasticsearch_connect


def transform_data(mongodb_data):

    # Remove '_id' and update Series_title_4 and Series_title_5 as needed
    modified_data = [
        {
            **{k: v for k, v in item.items() if k != '_id'},
            'Series_title_4': 'Updated Value 4' if item.get('Series_title_4') == '' else item['Series_title_4'],
            'Series_title_5': 'Updated Value 5' if item.get('Series_title_5') == '' else item['Series_title_5']
        }
        for item in mongodb_data
    ]
    return modified_data

def fetch_data(mongodb_coll):
    records = mongodb_coll.find({})
    mongodb_data = [rec for rec in records]
    updated_data = transform_data(mongodb_data)
    return updated_data

def read_csv(file_path):
    mongodb_coll = mongo_connect()

    # Accumulate rows to be inserted
    data_to_insert = []

    with open(file_path, mode='r') as file:
        csv_reader = csv.DictReader(file)
        for row in csv_reader:
            data_to_insert.append(row)

    # Insert all rows into MongoDB
    if data_to_insert:
        mongodb_coll.insert_many(data_to_insert)

    # Fetch the data from MongoDB
    final_data = fetch_data(mongodb_coll)

    es_conn = elasticsearch_connect()
    success, failed = bulk(es_conn, final_data, index="business_csv_data") 
    print(f"Successfully inserted {success} documents.")


if __name__ == "__main__":
    file_path = "/home/shubhangilamkhade/Downloads/business_data.csv"
    read_csv(file_path)
