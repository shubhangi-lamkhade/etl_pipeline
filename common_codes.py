from pymongo import MongoClient
from elasticsearch import Elasticsearch


def mongo_connect():

    mongoClient = MongoClient(MONGO_URL)
    db = mongoClient['Land_Records']
    collection = db['Employee']
    return collection

def elasticsearch_connect():

    es = Elasticsearch(ELASTIC_SEARCH)
    if es.ping():                         #.ping is the elasticsearch method to check the cluster is reachable or not
        print("Connected to Elasticsearch")
    else:
        print("Could not connect to Elasticsearch")
        