# -*- coding: utf-8 -*-

import os
import shutil
import time
import json
import glob
from itertools import chain
from werkzeug.utils import secure_filename
from pathlib import Path
import geopandas as gpd
from shapely.geometry import Polygon
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.ext.declarative import declarative_base
from geoalchemy2 import Geometry

from flask import Flask, request, jsonify
from flask_cors import CORS

from utils import *
from config import Config, ENGINE


app = Flask(__name__)
cors = CORS(app)
client = app.test_client()

session = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=ENGINE))


Base = declarative_base()
Base.query = session.query_property()

from models import *

app.config.from_object(Config)

logger = get_logger(__name__)

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


@app.teardown_appcontext
def shutdown_session(exception=None):
    session.remove()


@app.route("/api")
def hello():
    # do your things here
    return "It works!"



@app.route('/api/upload', methods=['POST'])
def upload_file():
    user = request.headers.get('User')
    directory = Path(Config.UPLOAD_FOLDER, user)
    directory.mkdir(exist_ok=True, parents=True)
    for f in request.files.getlist('file'):
        filename = secure_filename(f.filename)
        #проверка разных расширений
        for pattern in Config.ALLOWED_EXTENSIONS:
            if filename.endswith(pattern):
                f.save(str(directory / filename))
    return {'message': 'ok'}


@app.route('/api/remove/<filename>', methods=['GET'])
def remove_file(filename):
    user = request.headers.get('User')
    filename = secure_filename(filename)
    file = Path(Config.UPLOAD_FOLDER, user, filename)
    #проверка разных расширений
    for pattern in Config.ALLOWED_EXTENSIONS:
        if filename.endswith(pattern):
            try:
                os.remove(file)
                return {"status": "ok"}
            except:
                return {"status": "bad"}
    return {"status": "bad"}


@app.route('/api/load_data', methods=['GET'])
def load_file():
    user = request.headers.get('User')
    result = {'status': 'ok', 'messages': []}  
    folder = Path(Config.UPLOAD_FOLDER, user)
    shapefiles = glob.iglob(str(folder) + '/*.shp')
    geopackages = glob.iglob(str(folder) + '/*.gpkg')
    files = chain(shapefiles, geopackages) # generator
    for file in files:
        try:
            gdf = gpd.read_file(file).to_crs(4326)
            layer= Path(file).name.split(".")[0]
            print(layer)
            gdf.to_postgis(layer, ENGINE, if_exists='append', index=False, schema='gpzu_ninja',
                dtype={'geometry': Geometry(geometry_type='MULTIPOLYGON', srid=4326)})
            logger.info(f'success with file {layer}')
        except:
            result['messages'].append(f"Не удалось загрузить файл {file}")
            logger.info(f"Не удалось загрузить файл {file}")
    if folder.exists():
        try:
            shutil.rmtree(folder)
        except PermissionError:
            print('cannot remove folder')
    return result


@app.route('/api/krt_in_boundaries', methods=['POST'])
def calculate_krt():
    data = json.loads(request.data)
    polygon = data['polygon']
    criteria = data['criteria']
    params = request.args
    [l, t, r, b] = [float(x) for x in params.get('bbox').split(',')]
    zu = gpd.read_postgis("""
        select * from gpzu_ninja.zu
        where gpzu_ninja.zu.geometry && ST_MakeEnvelope(%s, %s, %s, %s, 4326)
    """ %(t, l, b, r), ENGINE, geom_col='geometry', crs=4326)
    oks = gpd.read_postgis("""
        select * from gpzu_ninja.oks
        where gpzu_ninja.oks.geometry && ST_MakeEnvelope(%s, %s, %s, %s, 4326)
    """ %(t, l, b, r), ENGINE, geom_col='geometry', crs=4326)
    coordinates = Polygon(polygon["features"][0]["geometry"]['coordinates'][0])
    polygon = gpd.GeoDataFrame(polygon, geometry=[coordinates], index=[0]).set_crs(4326)
    print(list(polygon.geometry)[0].wkt)
    int_zu = gpd.sjoin(zu, polygon, op='intersects').query('index_right == 0').drop(columns=['index_right'])
    int_zu['total_index'] = calculate_criteria(int_zu, criteria, 'zu')
    zu_included = int_zu.query("total_index >= 0.5")
    zu_discussed = int_zu.query("0.19 <= total_index < 0.5")
    ##выборка оксов по отобранным зу
    int_oks = gpd.sjoin(oks, zu_included, op='intersects').query('index_right == 0').drop(
        columns=['index_right', 'cadnum_right', 'okn_right', 'descr_right', 'kvartal_cn_right', 
        'kvartal_cn_right', 'cad_cost_right', 'id_right', 'address_right', 'area_value_right', 'kvartal_right', 
        'fid_right', 'kol_mest_right', 'okn_right', 'szz_right', 'samovol_right', 'rental_right']
    )
    int_oks.columns = ['cadnum', 'address', 'Area', 'descr', 'area_value',
       'cad_cost', 'cc_date_entering', 'cn', 'floors', 'id',
       'kvartal', 'kvartal_cn', 'name', 'oks_type', 'purpose',
       'purpose_name', 'reg_date', 'year_built', 'geometry', 'fid',
       'szz', 'kol_mest', 'okn', 'accident', 'rennovation',
       'typical', 'labour_small', 'labour_medium', 'labour_large',
       'samovol', 'living', 'rental', 'non_vri', 'has_effecct',
       'property_t', 'shape_area', 'cc_date_entering_right', 'category_type',
       'area_type', 'util_by_doc', 'parcel_rent',
       'parcel_owned', 'parcel_vri', 'type', 'features', 'total_index']
    int_oks['total_index'] = calculate_criteria(int_oks, criteria, 'oks')
    oks_included = int_oks.query("total_index >= 0.5")
    oks_discussed = int_oks.query("0.19 <= total_index < 0.5")
    print(len(zu_included), len(oks_included))

    print('int_zu : %s. included: %s, %s discussed' % (int_zu.shape, len(zu_included), len(zu_discussed)))
    print('int_oks : %s. included: %s, %s discussed' % (int_oks.shape, len(oks_included), len(oks_discussed)))
    #собираем сдисолвленный слой из геометрии и вычисленных атрибутов
    int_zu['fid'] = int_zu['fid'].astype('int64')
    int_zu.to_file(Path(Config.UPLOAD_FOLDER, 'output_zu.gpkg'), driver="GPKG")
    oks['fid'] = oks['fid'].astype('int64')
    oks.to_file(Path(Config.UPLOAD_FOLDER, 'output_oks.gpkg'), driver="GPKG")
    if len(zu_included) > 0:
        krt = dissolve_geometry(zu_included)
        print('krt', krt.shape)
        krt.to_excel(Path(Config.UPLOAD_FOLDER, 'output_krt.xlsx'))
        #krt['zu_list'] = [parcels_in_boundaries(x, zu_included) for x in krt['geometry']]
        #krt['oks_list'] = [parcels_in_boundaries(x, oks_included) for x in krt['geometry']]
        #посчитать площадь крт
    else:
        krt = gpd.GeoDataFrame({'geometry':[]})
    zu_list = zu_included['cadnum'].dropna().to_list()
    zu_list.sort()
    oks_list = oks_included['cadnum'].dropna().to_list()
    oks_list.sort()
    result = {
        'krt': krt.to_json(),
        'zu': zu_list,
        'oks': oks_list,
        'zu_discussed': zu_discussed.to_json(),
        'oks_discussed': oks_discussed.to_json(),
    }

    return result
    

@app.route('/api/layer/<layer_name>', methods=['POST'])
def return_layer(layer_name):
    data = json.loads(request.data)
    if layer_name not in ('zu', 'oks', 'nerazgr', 'okn', 'szz', 'start_area'):
        return {}
    if len(data) > 0:
        cn_list = "('%s')" % data[0] if len(data) == 1 else str(tuple(data))
        layer = gpd.read_postgis("""
            select *, '%s' as layer_name from gpzu_ninja.%s where cadnum in %s
        """ % (layer_name, layer_name, cn_list), ENGINE, geom_col='geometry', crs=4326)
    elif len(data) == 0 and layer_name in ('nerazgr', 'okn', 'szz', 'start_area'):
        layer = gpd.read_postgis("""
            select *, '%s' as layer_name from gpzu_ninja.%s
        """ % (layer_name, layer_name), ENGINE, geom_col='geometry', crs=4326)
    else:
        layer = gpd.GeoDataFrame({'geometry':[]})
    return layer.to_json()


@app.route("/api/search", methods=['GET'])
def search():
    pattern = request.args.get('pattern').lower().strip()
    matches = AllParcels.query.filter(
        (AllParcels.cn.ilike(f'{pattern}%')) | 
        (AllParcels.cn.ilike(f'%{pattern}%')) | 
        (AllParcels.cn.ilike(f'%{pattern}'))
    ).limit(5)
    serialized = []
    for row in matches:
        serialized.append({
            'cadnumber': row.cn,
            'p_type': row.p_type
        })
    return jsonify(serialized)



if __name__ == "__main__":
    app.run()