import logging
import numpy as np
from datetime import datetime
from shapely.ops import unary_union
from shapely.geometry import Polygon, MultiPolygon
from geopandas import GeoDataFrame, sjoin, read_postgis
from pandas import Series, DataFrame, ExcelWriter
from config import Config, ENGINE
from pathlib import Path
import xlsxwriter


title = 'Обзор сформированного КРТ'
property_t = {'0': 'информация о собственности отсутствует', '1': 'Москва', '2': 'РФ', '3': 'Иная', '5':'Неразграниченная'}
fields_dict =  {
    'cadnum': 'Кад.номер', 'has_effecct': 'Признак аренды', 'property_t': 'Собственность', 
    'shape_area': 'Площадь', 'kvartal_cn': 'Кад.квартал', 'cad_cost': 'Кад.стоимость', 
    'category_type': 'Категория земель', 'address': 'Адрес', 'util_by_doc': 'По документу', 'purpose_name': 'ВРИ'
}
zu_fields = ['cadnum', 'has_effecct', 'property_t', 'shape_area', 'kvartal_cn', 'cad_cost', 'category_type', 'address', 'util_by_doc']
oks_fields = ['cadnum', 'shape_area', 'kvartal_cn', 'cad_cost', 'category_type', 'address', 'purpose_name']


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

        total_index = total_zu + total_rights
    
    if parcel_type == 'oks':
        columns = ['accident', 'labour_small', 'labour_medium', 'labour_large', 'okn', 'szz', 'rental']
        for col in columns:
            gdf[col] = np.where(gdf['living'] == 1, l[col] * gdf[col], n_l[col] * gdf[col])
        gdf['rennovation'] = np.where(gdf['living'] == 1, 
            l['rennovation'] * gdf['rennovation'], n_l['non_vri'] * gdf['non_vri']
        )
        gdf['typical'] = np.where(gdf['living'] == 1, 
            l['typical'] * gdf['typical'], n_l['samovol'] * gdf['samovol']
        )
        total_living = np.where(gdf['living'] == 1, 
            l['total_living'] * (gdf['accident'] + gdf['rennovation'] + gdf['typical']), 
            n_l['total_non_living'] * (gdf['accident'] + gdf['rennovation'] + gdf['typical'])
        )
        total_labour = np.where(gdf['living'] == 1, 
            l['total_labour'] * (gdf['labour_small'] + gdf['labour_medium'] + gdf['labour_large']), 
            n_l['total_labour'] * (gdf['labour_small'] + gdf['labour_medium'] + gdf['labour_large'])
        )
        total_rights = np.where(gdf['living'] == 1, 
            l['total_rights'] * (gdf['okn'] + gdf['szz'] + gdf['rental']), 
            n_l['total_rights'] * (gdf['okn'] + gdf['szz'] + gdf['rental'])
        )

        total_index = Series(total_living + total_labour + total_rights)

        gdf['total_living'] = total_living
        gdf['total_labour'] = total_labour
        gdf['total_rights'] = total_rights
        gdf['total_index'] = total_index
        gdf.to_excel(Path(Config.UPLOAD_FOLDER, 'output_oks.xlsx'))
    
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


def set_property(row):
    value = str(row['property_t'])
    try:
        return property_t[value]
    except:
        return 'информация о собственности отсутствует'


def make_pdf(zu_list, oks_list):
    if len(zu_list) == 0:
        return
    zu_list = "('%s')" % zu_list[0] if len(zu_list) == 1 else str(tuple(zu_list))

    zu = read_postgis("""
        select * from gpzu_ninja.zu where cadnum in %s
    """ % (zu_list), ENGINE, geom_col='geometry', crs=4326).to_crs(3857)

    oks_attrs = DataFrame()
    if len(oks_list) > 0:
        oks_list = "('%s')" % oks_list[0] if len(oks_list) == 1 else str(tuple(oks_list))
        oks = read_postgis("""
            select * from gpzu_ninja.zu where cadnum in %s
        """ % (oks_list), ENGINE, geom_col='geometry', crs=4326).to_crs(3857)
        oks_attrs = oks.drop(columns=["geometry"])

    zu_attrs = zu.drop(columns=["geometry"])

    final = {
       'ЗУ в КРТ' : zu_attrs[zu_fields].rename(fields_dict, axis=1),
       'ОКС в КРТ' : oks_attrs.rename(fields_dict, axis=1),
    }
    filename = f'report_krt_{datetime.now().strftime("%Y%m%d_%H%M%S")}.xlsx'
    writer = ExcelWriter(Config.UPLOAD_FOLDER + '/' + filename, engine='xlsxwriter')
    for sheet_name in final:
        final[sheet_name].to_excel(writer, sheet_name= str(sheet_name))
    writer.save()

    return filename