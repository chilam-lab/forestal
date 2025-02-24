import sys
import os
from dotenv import load_dotenv
import json
from pymongo import MongoClient

load_dotenv()

MONGODB_URI = os.getenv('MONGODB_URI')

# Conexión a la base de datos
client = MongoClient(MONGODB_URI)
db = client['sembrando_vida']
counter = db['counter']
variables = db['variables']

def getNextValue() -> int:
    """
    Incrementa el contador de id's en la base de datos y devuelve el nuevo valor.

    Returns
    -------
    `int`
        Valor del campo **_id** para la nueva variable que se agregará a la base de datos.
    
    """
    counter.find_one_and_update({"_id":"variable_id"}, {"$inc":{"value":1}})
    count = counter.find_one({})
    return count['value']

def appendID(data: list[dict]) -> list[dict]:
    """
    Agrega un campo **_id** a cada variable que se agregará a la base de datos.

    Parameters
    ----------
    data : `list` [`dict`]
        Lista de diccionarios (procesado de un archivo JSON) a los que se agregará el campo **_id**.

    Returns
    -------
    `list` [`dict`]
        Lista de diccionarios con el campo **_id** agregado.
    """

    for i in data:
        newId = getNextValue()
        i['_id'] = newId
    return data

if __name__ == "__main__":

    file = sys.argv[1]

    with open(file, mode='r') as jsonfile:
        data = json.load(jsonfile)
        data = appendID(data)
        variables.insert_many(data)
