import logging
import numpy as np
from shapely.ops import unary_union
from shapely.geometry import Polygon, MultiPolygon
from geopandas import GeoDataFrame, sjoin
from pandas import Series

from config import Config
from pathlib import Path

def get_logger(name):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    file_handler = logging.FileHandler('action.log', mode='a')
    stream_handler = logging.StreamHandler()
    formatter = logging.Formatter('[%(name)s] %(asctime)s %(levelname)-8s %(message)s')
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.INFO)
    stream_handler.setFormatter(formatter)
    stream_handler.setLevel(logging.ERROR)
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    return logger


def calculate_criteria(gdf, criteria, parcel_type):
    for k, v in criteria.items():
        for key, value in v.items():
            criteria[k][key] = int(value)/100
    l = criteria['living']
    n_l = criteria['non_living']
    if parcel_type == 'zu':
        z = l['parcel_rent'] * gdf['parcel_rent'] + l['parcel_owned'] * gdf['parcel_owned'] + l['parcel_vri'] * gdf['parcel_vri']
        total_zu = l['total_parcel'] * z

        r = l['okn'] * gdf['okn'] + l['szz'] * gdf['szz'] + l['rental'] * gdf['rental']
        total_rights = l['total_rights'] * r

        total_index = total_zu * total_rights

        #gdf['total_zu'] = total_zu
        #gdf['total_rights'] = total_rights
        #gdf['total_index'] = total_index
        #gdf.to_excel(Path(Config.UPLOAD_FOLDER, 'output_zu.xlsx'))
    
    if parcel_type == 'oks':
        columns = ['accident', 'labour_small', 'labour_medium', 'labour_large', 'okn', 'szz', 'rental']
        for col in columns:
            gdf[col] = np.where(gdf['living'] == True, l[col] * gdf[col], n_l[col] * gdf[col])
        gdf['rennovation'] = np.where(gdf['living'] == True, l['rennovation'] * gdf['rennovation'], n_l['non_vri'] * gdf['non_vri'])
        gdf['typical'] = np.where(gdf['living'] == True, l['typical'] * gdf['typical'], n_l['samovol'] * gdf['samovol'])
        total_living = np.where(gdf['living'] == True, 
            l['total_living'] * (gdf['accident'] + gdf['rennovation'] + gdf['typical']), 
            n_l['total_non_living'] * (gdf['accident'] + gdf['rennovation'] + gdf['typical'])
        )
        total_labour = np.where(gdf['living'] == True, 
            l['total_labour'] * (gdf['labour_small'] + gdf['labour_medium'] + gdf['labour_large']), 
            n_l['total_labour'] * (gdf['labour_small'] + gdf['labour_medium'] + gdf['labour_large'])
        )
        total_rights = np.where(gdf['living'] == True, 
            l['total_rights'] * (gdf['okn'] + gdf['szz'] + gdf['rental']), 
            n_l['total_rights'] * (gdf['okn'] + gdf['szz'] + gdf['rental'])
        )

        total_index = Series(total_living * total_labour * total_rights)

        #gdf['total_living'] = total_living
        #gdf['total_labour'] = total_labour
        #gdf['total_rights'] = total_rights
        #gdf['total_index'] = total_index
        #gdf.to_excel(Path(Config.UPLOAD_FOLDER, 'output_oks.xlsx'))
    
    return total_index


def dissolve_geometry(gdf):
    e = gdf.explode()
    geometry_rows = e.geometry.to_list()
    geometry = MultiPolygon(geometry_rows)
    new_geom = unary_union(geometry.buffer(0)) #.00005
    if isinstance(new_geom, Polygon):
        empty_attrs = ['krt']
    else:
        empty_attrs = ['krt' for i in  range(len(new_geom))]
    d = {'layer_name': empty_attrs, 'geometry': new_geom}
    gdf = GeoDataFrame(d, crs="EPSG:4326")
    return gdf


def parcels_in_boundaries(krt_polygon, parcels):
    krt_polygon = GeoDataFrame({'geometry': krt_polygon}, index=[0], crs="EPSG:4326")
    intersection = sjoin(parcels, krt_polygon, op='intersects').query('index_right == 0')
    if 'cadnum_left' in intersection.columns:
        frame = intersection['cadnum_left']
    else:
        frame = intersection['cadnum']
    return list_to_string(frame)


def list_to_string(l):
    l = l.dropna()
    return ", ".join(l) if type(l) == list else ", ".join(l.tolist())