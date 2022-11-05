import os
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()

class Config:
    DB_HOST = os.getenv('DB_HOST') 
    DB_PORT = os.getenv('DB_PORT')
    DB_NAME = os.getenv('DB_NAME')
    DB_USER = os.getenv('DB_USER')
    DB_PASS = os.getenv('DB_PASS')
    DB_URI = 'postgresql://{}:{}@{}:{}/{}'.format(DB_USER, DB_PASS, DB_HOST, DB_PORT, DB_NAME)
    UPLOAD_FOLDER = os.path.dirname(os.path.realpath(__file__)) + '/upload/'
    CORS_ALLOWED_ORIGINS = ['http://localhost:4200']
    ALLOWED_EXTENSIONS = ['.shp', '.dbf', '.prj', '.sbn', '.sbx', '.shx', '.shp.xml', '.gpkg']

ENGINE = create_engine(Config.DB_URI)