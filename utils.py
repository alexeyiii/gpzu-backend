import logging
from shapely.ops import unary_union
from shapely.geometry import Polygon, MultiPolygon
from geopandas import GeoDataFrame, sjoin

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
    total_rights = 1
    if parcel_type == 'zu':
        total_living = 1
        total_labour = 1
        total_ownership = 0.55
    elif parcel_type == 'oks':
        total_living = 0.55
        total_labour = 0.55
        total_ownership = 1
    total_index = total_living * total_labour * total_rights * total_ownership
    return round(total_index, 4)


def dissolve_geometry(gdf):
    e = gdf.explode()
    geometry_rows = e.geometry.to_list()
    geometry = MultiPolygon(geometry_rows)
    new_geom = unary_union(geometry.buffer(0)) #.00005
    if isinstance(new_geom, Polygon):
        empty_attrs = ['']
    else:
        empty_attrs = ['' for i in  range(len(new_geom))]
    d = {'zu_list': empty_attrs, 'oks_list': empty_attrs, 'geometry': new_geom}
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