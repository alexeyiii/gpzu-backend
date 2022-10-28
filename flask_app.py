# -*- coding: utf-8 -*-

import os
import time
from werkzeug.utils import secure_filename
from pathlib import Path
import geopandas as gpd
from geoalchemy2 import Geometry

from flask import Flask, request
from flask_cors import CORS

import utils
from config import Config, ENGINE


app = Flask(__name__)
cors = CORS(app)


client = app.test_client()
app.config.from_object(Config)

logger = utils.get_logger(__name__)

@app.before_request
def reqbeg():
    request.beg = time.time()
    user = " by " + request.headers.get('User') if request.headers.get('User') else " by unknown_user"
    logger.info('begin: %s %s%s', request.method, request.path, user)

@app.after_request
def reqend(response):
    user = " by " + request.headers.get('User') if request.headers.get('User') else " by unknown_user"
    logger.info('end: %s %s%s => %s in %.5fs', request.method, request.path, user,
                    response.status_code, time.time() - request.beg)
    return response

@app.teardown_request
def reqtear(error = None):
    if error:
        user = " by " + request.headers.get('User') if request.headers.get('User') else " by unknown_user"
        logger.exception('error: %s %s%s in %.5fs:\n%s', request.method, request.path, user,
                         time.time() - request.beg, error)


@app.route("/api")
def hello():
    # do your things here
    return "It works!"



@app.route('/api/upload', methods=['POST'])
def upload_file():
    user = request.headers.get('pid')
    directory = Path(Config.UPLOAD_FOLDER, user)
    for f in request.files.getlist('file'):
        filename = secure_filename(f.filename)
        #проверка разных расширений
        if filename.endswith('.zip') or filename.endswith('.gpkg'):
            f.save(str(directory / filename))
    return {'message': 'ok'}


@app.route('/api/remove/<filename>', methods=['GET'])
def remove_file(filename):
    user = request.headers.get('pid')
    filename = secure_filename(filename)
    file = Path(Config.UPLOAD_FOLDER, user, filename)
    #проверка разных расширений
    if filename.endswith('.zip') or filename.endswith('.gpkg'):
        try:
            os.remove(file)
            return {"status": "ok"}
        except:
            return {"status": "bad"}
    return {"status": "bad"}


@app.route('/api/load_data/<parcel_type>', methods=['GET'])
def load_file(parcel_type):
    user = request.headers.get('pid')
    if parcel_type not in ('zu', 'oks'):
        return {'status': 'bad', 'error': 'parcel_type not supported'}
    folder = Path(Config.UPLOAD_FOLDER, user)
    gdf = gpd.read_file(folder).to_crs(4326)
    gdf.to_postgis(parcel_type, ENGINE, if_exists='append', index=False,
        dtype={'geometry': Geometry(geometry_type='MULTIPOLYGON', srid=4326)})
    return {'status': 'ok'}