from zdatabase import db
from zdatabase.mixins import DatabaseMixin, QueryMixin, MapperMixin
from zdatabase.utils import time


class Model(DatabaseMixin, QueryMixin, MapperMixin, db.Model):
    __abstract__ = True
    __schema__ = {}

    @classmethod
    def filter(cls, *args, **kwargs):
        return cls.query.filter(*args, **kwargs)

    @classmethod
    def clean_params(cls, data: dict):
        data = data.copy()
        keys = list(data.keys())
        for key in keys:
            if key not in cls.keys():  # 无关字段过滤
                data.pop(key)
        return data

    @classmethod
    def keys(cls):
        return cls.__table__.columns.keys()

    def get(self):
        self.__getattribute__(key)

    @property
    def key_map(self):
        """字段转化"""
        return {}

    @property
    def key_derive_map(self):
        """生成冗余字段"""
        return {}

    def raw_json(self) -> dict:
        """"""
        return {key: self.get(key) for key in self.keys()}

    def to_json(self) -> dict:
        return self.to_json_()

    def to_json_(self) -> dict: 
        tmp = {}
        for k in self.keys():
            v = self.get(k)
            tmp[k] = v
            for key, func in self.key_map.items():
                if k == key:
                    tmp[k] = func(v)
            for key, map in self.key_derive_map.items():
                if k == key:
                    if isinstance(map, list):
                        for map_ in map:
                            name = map_['name']
                            func = map_['func']
                            tmp[name] = func(v)
                    else:
                        name = map['name']
                        func = map['func']
                        tmp[name] = func(v)
        return tmp

    @staticmethod
    def format_date(value) -> str:
        return time.format(value, 'YYYY-MM-DD') if value is not None else ''

    @staticmethod
    def format_datetime(value) -> str:
        return time.format(value, 'YYYY-MM-DD HH:mm:ss') if value is not None else ''

    @classmethod
    def new(cls, data: dict) -> Model:
        return cls(**cls.clean_params(data))

    def add_one(self) -> Model:
        """添加到会话"""
        db.session.add(self)
        db.session.flush()
        return self

    def update(self, data: dict) -> Model:
        """修改信息"""
        dict_ = self.clean_params(data)
        for k, v in data.items():
            setattr(self, k, v)
        return self

    def merge(self) -> Model:
        """合并到会话"""
        db.session.merge(self)
        return self
