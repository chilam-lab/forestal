import os
import sys
from dotenv import load_dotenv
from flask import Flask, jsonify, request
from pymongo import MongoClient

app = Flask(__name__)

load_dotenv()

MONGODB_URI = os.getenv('MONGODB_URI')
print(MONGODB_URI)

# MongoDB connection
client = MongoClient(MONGODB_URI)
db = client['sembrando_vida']
variables = db['variables']


@app.route('/variables')
def get_variables():
    """
    Obtiene una lista con todas las variables en la base de datos.

    Endpoint
    --------
    /variables
    
    Returns
    -------
    `list` [`dict`]
        Un arreglo JSON con **id**: identificador de la variable, **name**: nombre de la variable,
        **level_size**: número de niveles que toma la variable, **filter_fields**: campos que
        acepta la variable a filtrar y **available_grids**: listado de mallas disponibles para la
        variable.
    """
    vars = db.variables.aggregate([{"$project": {"_id":0, "id": "$_id", "name": "$name"}},
                                   {"$addFields":{"level_size":0,
                                                  "available_grids":["mun"],
                                                  "filter_fields":[]}}])
    return jsonify(vars.to_list())

@app.route('/variables/<id>')
def variables_id(id:int) -> any:
    """
    Obtiene el listado de niveles de la variable con el id dado.
    
    Parameters
    ----------
    id : `int`
        Identificador de la variable.

    Endpoint
    --------
    /variables/:id:

    /variables/:id:?q=:q:&offset=:offset:&limit=:limit:
    
    Returns
    -------
    `list` [`dict`]
        Un arreglo JSON con **id**: id de la variable, **level_id**: identificador del nivel de valor de
        la variable y **data**: información sobre el nivel de la variable.
    """
    q = request.args.get('q', '*')
    offset = request.args.get('offset', 0)
    limit = request.args.get('limit', 10)
    variable_id = db.variables.aggregate([{"$match":{"_id":int(id)}},
                                        {"$project": {"_id":0, "id": "$_id","data": "$name"}},
                                        {"$addFields":{"level_id":0}},
                                        {"$limit":limit},
                                        {"$skip":offset}])
    variables = variable_id.to_list()
    if not variables:
        return jsonify({'error': 'variable id not found'}), 404

    return jsonify(variables)
    

@app.route('/get-data/<id>')
def get_data(id):
    """
    Obtiene los valores de los niveles solicitados de la variable con el id dado.
    
    Parameters
    ----------
    id : `int`
        Identificador de la variable.
    
    Endpoint
    --------
    /get-data/:id:?grid_id=:grid_id:

    /get-data/:id:?grid_id=:grid_id:&levels_id=:levels_id:&filter_names=:filter_names:&filter_values=:filter_values:
    
    Returns
    -------
    `list` [`dict`]
        Un arreglo JSON con **id**: id de la variable, **level_id**: identificador del nivel de valor
        de la variable, **grid_id**: identificador de la malla, **cells**: lista de celdas de la malla
        donde toma el nivel la variable y **n**: número de celdas ocupadas por el nivel del valor de
        la variable
    """
    grid_id = request.args.get('grid_id')
    levels_id = request.args.get('levels_id', type=lambda v: v.split(','))
    filter_names = request.args.get('filter_names', type=lambda v: v.split(','))
    filter_values = request.args.get('filter_values', type=lambda v: v.split(','))

    if not grid_id:
        return jsonify({'error': 'grid_id is required'}), 400

    data_id = db.variables.aggregate([{"$match":{"_id":int(id)}},
                                     {"$project":{"_id":0, "id": "$_id",
                                                  "cells":"$cells",
                                                  "n":{"$size":"$cells"}}},
                                     {"$addFields":{"grid_id":str(grid_id),"level_id":0,}}])
    data = data_id.to_list()
    if not data:
        return jsonify({'error': 'variable id not found'}), 404
    
    return jsonify(data)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=2112)