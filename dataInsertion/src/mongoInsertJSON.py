import os
from dotenv import load_dotenv
import json
from pymongo import MongoClient

load_dotenv()

MONGODB_URI = os.getenv('MONGODB_URI')

# MongoDB connection
client = MongoClient(MONGODB_URI)
db = client['sembrando_vida']
counter = db['counter']
variables = db['variables']
file = 'Politic/politic12.json'

def getNextValue() -> int:
    counter.find_one_and_update({"_id":"variable_id"}, {"$inc":{"value":1}})
    count = counter.find_one({})
    return count['value']

def appendID(data: list[dict]) -> list[dict]:
    for i in data:
        newId = getNextValue()
        i['_id'] = newId
    return data

with open(file, mode='r') as jsonfile:
    data = json.load(jsonfile)
    data = appendID(data)
    variables.insert_many(data)
