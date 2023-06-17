from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.ext.declarative import declarative_base


class Database:
    def init(self, config):
        url = 'mysql+pymysql://{user}:{password}@{host}:{port}/{db}?charset=utf8mb4'.format(**config)
        engine = create_engine(url)
        metadata = MetaData(bind=engine)
        Session = sessionmaker(autocommit=False, autoflush=False, bind=engine)
        self.session = Session()
        return engine, metadata
 
 
Base = declarative_base()
db = Database()
