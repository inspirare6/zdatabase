from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import MetaData, create_engine


class Database(SQLAlchemy):
    def make_url(self, config, db_type):
        if db_type == 'postgre':
            url = 'postgresql://{user}:{password}@{host}:{port}/{db}'.format(**config)
        else:
            url = 'mysql+pymysql://{user}:{password}@{host}:{port}/{db}?charset=utf8mb4'.format(**config)
        return url

    def init(self, config, db_type):
        url = self.make_url(config, db_type)
        engine = create_engine(url)
        metadata = MetaData(bind=engine)
        return engine, metadata
 
    def mount(self, app):
        super().init_app(app)


db = Database()
session = db.session
