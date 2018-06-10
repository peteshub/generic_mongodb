from pymongo import MongoClient


def getMongoDB():
    client = MongoClient('localhost', 27017)
    return client

