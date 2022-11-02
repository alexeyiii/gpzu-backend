import sqlalchemy as db
from flask_app import Base
from geoalchemy2 import Geometry


class ZuLayer(Base):
    __tablename__ = 'zu'
    __table_args__ = {"schema": "gpzu_ninja"}
    descr = db.Column(db.String(250), nullable=False, primary_key=True)
    has_effecct = db.Column(db.Integer, nullable=True)
    property_t = db.Column(db.Integer, nullable=True)
    shape_area = db.Column(db.Float, nullable=True) 
    cc_date_entering  = db.Column(db.String(250), nullable=True)
    kvartal_cn = db.Column(db.String(250), nullable=True)
    cad_cost = db.Column(db.Float, nullable=True) 
    id = db.Column(db.String(250), nullable=True)
    category_type = db.Column(db.String(250), nullable=True)
    address = db.Column(db.String(250), nullable=True)
    area_value = db.Column(db.Float, nullable=True) 
    kvartal = db.Column(db.String(250), nullable=True)
    area_type = db.Column(db.String(250), nullable=True)
    cn = db.Column(db.String(250), nullable=True)
    util_by_doc = db.Column(db.String(250), nullable=True)
    geometry = db.Column(Geometry(geometry_type='MULTIPOLYGON'))


class OksLayer(Base):
    __tablename__ = 'oks'
    __table_args__ = {"schema": "gpzu_ninja"}
    cadnum = db.Column(db.String(250), nullable=False, primary_key=True)
    address = db.Column(db.String(250), nullable=True)
    area = db.Column(db.Float, nullable=True) 
    descr = db.Column(db.String(250), nullable=True)
    area_value = db.Column(db.Float, nullable=True) 
    cad_cost = db.Column(db.Float, nullable=True) 
    cc_date_entering = db.Column(db.String(250), nullable=True)
    cn = db.Column(db.String(250), nullable=True)
    floors = db.Column(db.String(250), nullable=True)
    id  = db.Column(db.String(250), nullable=True)
    kvartal = db.Column(db.String(250), nullable=True)
    kvartal_cn = db.Column(db.String(250), nullable=True)
    name   = db.Column(db.String(250), nullable=True)
    oks_type = db.Column(db.String(250), nullable=True)
    purpose = db.Column(db.String(250), nullable=True)
    purpose_name = db.Column(db.String(250), nullable=True)
    reg_date = db.Column(db.String(250), nullable=True)
    year_built = db.Column(db.Float, nullable=True) 
    geometry = db.Column(Geometry(geometry_type='MULTIPOLYGON'))


class AllParcels(Base):
    __tablename__ = 'all_parcels'
    __table_args__ = {"schema": "gpzu_ninja"}
    fid = db.Column(db.Integer, nullable=False, primary_key=True)
    cn = db.Column(db.String(250), nullable=False)
    p_type = db.Column(db.String(250), nullable=False)