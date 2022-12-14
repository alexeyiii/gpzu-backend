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

from flask import Flask, request, jsonify, send_from_directory, abort
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

oks_dict = {'cadnum_left': 'cadnum', 'address_left': 'address', 'Area': 'Area', 'descr_left': 'descr', 'area_value_left': 'area_value', 'cad_cost_left': 'cad_cost', 'cc_date_entering_left': 'cc_date_entering', 'cn': 'cn', 'floors': 'floors', 'id_left': 'id', 'kvartal_left': 'kvartal', 'kvartal_cn_left': 'kvartal_cn', 'name': 'name', 'oks_type': 'oks_type', 'purpose': 'purpose', 'purpose_name': 'purpose_name', 'reg_date': 'reg_date', 'year_built': 'year_built', 'geometry': 'geometry', 'fid_left': 'fid', 'szz_left': 'szz', 'kol_mest_left': 'kol_mest', 'okn_left': 'okn', 'accident': 'accident', 'rennovation': 'rennovation', 'typical': 'typical', 'labour_small': 'labour_small', 'labour_medium': 'labour_medium', 'labour_large': 'labour_large', 'samovol_left': 'samovol', 'living': 'living', 'rental_left': 'rental', 'non_vri': 'non_vri', 'has_effecct': 'has_effecct', 'property_t': 'property_t', 'shape_area': 'shape_area', 'cc_date_entering_right': 'cc_date_entering_right', 'category_type': 'category_type', 'area_type': 'area_type', 'util_by_doc': 'util_by_doc', 'parcel_rent': 'parcel_rent', 'parcel_owned': 'parcel_owned', 'parcel_vri': 'parcel_vri', 'total_index_left': 'total_index'}


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
        #???????????????? ???????????? ????????????????????
        for pattern in Config.ALLOWED_EXTENSIONS:
            if filename.endswith(pattern):
                f.save(str(directory / filename))
    return {'message': 'ok'}


@app.route('/api/remove/<filename>', methods=['GET'])
def remove_file(filename):
    user = request.headers.get('User')
    filename = secure_filename(filename)
    file = Path(Config.UPLOAD_FOLDER, user, filename)
    #???????????????? ???????????? ????????????????????
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
            result['messages'].append(f"???? ?????????????? ?????????????????? ???????? {file}")
            logger.info(f"???? ?????????????? ?????????????????? ???????? {file}")
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
    for k, v in criteria.items():
        for key, value in v.items():
            criteria[k][key] = int(value)/100
    coordinates = Polygon(polygon["features"][0]["geometry"]['coordinates'][0])
    polygon = gpd.GeoDataFrame(polygon, geometry=[coordinates], index=[0]).set_crs(4326)
    p = list(polygon.geometry)[0].wkt
    start = time.perf_counter_ns()
    int_zu = gpd.read_postgis("""
        select * from gpzu_ninja.zu where ST_Intersects(zu.geometry, 'SRID=4326;%s')
    """ % p, ENGINE, geom_col='geometry', crs=4326).fillna(0)
    logger.info(f"{round((time.perf_counter_ns() - start)/10**9, 2)} sec. for zu load")
    oks = gpd.read_postgis("""
        select * from gpzu_ninja.oks where ST_Intersects(oks.geometry, 'SRID=4326;%s')
    """ % p, ENGINE, geom_col='geometry', crs=4326).fillna(0)
    int_zu['total_index'] = calculate_criteria(int_zu, criteria, 'zu')
    int_zu['property_t'] = int_zu.apply(lambda row: set_property(row), axis=1)


    #?????????? ????????????????
    zu_included = int_zu.query("total_index >= 0.5")
    zu_discussed = int_zu.query("0.19 <= total_index < 0.5")
    zu_discussed['layer_name'] = 'zu'

    ##?????????????? ?????????? ???? ???????????????????? ????
    oks['total_index'] = calculate_criteria(oks, criteria, 'oks')
    int_oks = gpd.sjoin(oks, zu_included, op='intersects').query('index_right >= 0').drop(
        columns=['index_right', 'cadnum_right', 'okn_right', 'descr_right', 'kvartal_cn_right', 
        'kvartal_cn_right', 'cad_cost_right', 'id_right', 'address_right', 'area_value_right', 'kvartal_right', 
        'fid_right', 'kol_mest_right', 'okn_right', 'szz_right', 'samovol_right', 'rental_right', 'total_index_right']
    )
    int_oks = int_oks.rename(oks_dict, axis=1)

    #int_oks['total_index'] = calculate_criteria(int_oks, criteria, 'oks')
    oks_included = int_oks.query("total_index >= 0.5")
    oks_discussed = int_oks.query("0.19 <= total_index < 0.5")
    oks_discussed['layer_name'] = 'oks'

    logger.info('int_zu : %s. included: %s, %s discussed' % (int_zu.shape, len(zu_included), len(zu_discussed)))
    logger.info('int_oks : %s. included: %s, %s discussed' % (int_oks.shape, len(oks_included), len(oks_discussed)))
    
    #???????????????? ?????????????????????????? ???????? ???? ?????????????????? ?? ?????????????????????? ??????????????????
    if len(zu_included) > 0:
        krt = dissolve_geometry(zu_included)
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
    params = request.args
    if layer_name not in ('zu', 'oks', 'nerazgr', 'okn', 'szz', 'start_area'):
        return {}
    if layer_name in ('zu', 'oks') and len(data) > 0:
        cn_list = "('%s')" % data[0] if len(data) == 1 else str(tuple(data))
        layer = gpd.read_postgis("""
            select *, '%s' as layer_name from gpzu_ninja.%s where cadnum in %s
        """ % (layer_name, layer_name, cn_list), ENGINE, geom_col='geometry', crs=4326)
    elif layer_name in ('nerazgr', 'okn', 'szz', 'start_area'):
        [l, t, r, b] = [float(x) for x in params.get('bbox').split(',')]
        layer = gpd.read_postgis("""
            select *, '%s' as layer_name from gpzu_ninja.%s
            where gpzu_ninja.%s.geometry && ST_MakeEnvelope(%s, %s, %s, %s, 4326)
        """ % (layer_name, layer_name, layer_name, l, t,r, b), ENGINE, geom_col='geometry', crs=4326)
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


@app.route("/api/pdf", methods=['POST'])
def create_pdf():
    data = json.loads(request.data)
    filename = make_pdf(data['zu'], data['oks'])
    return {'status': 'ok', 'url': filename}


@app.route("/api/file/<filename>", methods=['GET'])
def return_file(filename):
    if filename.endswith(".xlsx"):
        return send_from_directory(directory=app.config['UPLOAD_FOLDER'], path=filename, as_attachment=True)
    else:
        abort(404)


if __name__ == "__main__":
    app.run()