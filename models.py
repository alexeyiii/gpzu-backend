import sqlalchemy as db
from flask_app import Base
from geoalchemy2 import Geometry

class AllParcels(Base):
    __tablename__ = 'all_parcels'
    __table_args__ = {"schema": "gpzu_ninja"}
    fid = db.Column(db.Integer, nullable=False, primary_key=True)
    cn = db.Column(db.String(250), nullable=False)
    p_type = db.Column(db.String(250), nullable=False)