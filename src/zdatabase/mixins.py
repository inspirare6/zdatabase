from zdatabase import db
from jsonschema import validate


class DatabaseMixin:
    @staticmethod
    def flush():
        """提交会话"""
        db.session.flush()

    @staticmethod
    def commit():
        """事务提交"""
        db.session.commit()

    @staticmethod
    def rollback():
        """事务回滚"""
        db.session.rollback()
    
    @staticmethod
    def merge(self):
        """合并"""
        db.session.merge(self)
        return self

    @staticmethod
    def query(*args, **kwargs):
        """查询"""
        return db.session.query(*args, **kwargs)

    @staticmethod
    def add_all(items):
        """添加多个对象到会话"""
        db.session.add_all(items)


class QueryMixin:
    @classmethod
    def select(cls, params, conds):
        """筛选(模糊匹配）
        ?name=1&asset_sn=2019-BG-5453
        """
        flts = []
        for cond in conds:
            value = params.get(cond)
            flts += [getattr(cls, cond).like(f'%{value}%')] if value else []
        return flts

    @classmethod
    def select_(cls, params, conds):
        """筛选(精确匹配）
        ?name=1&asset_sn=2019-BG-5453
        """
        flts = []
        for cond in conds:
            value = params.get(cond)
            flts += [getattr(cls, cond) == value] if value else []
        return flts

    @classmethod
    def select_date(cls, attr_name, params):
        """日期筛选"""
        flts = []
        start_date = params.get('start_date')
        end_date = params.get('end_date')
        flts += [getattr(cls, attr_name) >= start_date] if start_date else []
        flts += [getattr(cls, attr_name) <= end_date] if end_date else []
        return flts

    @staticmethod
    def all(cls, query, method='to_json'):
        """返回全部记录
        """
        items = query.all()
        return [getattr(item, method)() for item in items]

    @staticmethod
    def paginate(query, params, method='to_json'):
        """分页
        page_size=100&page_num=1
        """
        page_num = int(params.get('page_num', '1'))
        page_size = int(params.get('page_size', '10'))
        pagination = query.paginate(page_num, per_page=page_size)
        rst = {
            'items': [getattr(item, method)() for item in pagination.items],
            'total': pagination.total,
            'pages': pagination.pages
        }
        return rst


class MapperMixin:
    @staticmethod
    def jsonlize(items):
        return [item.to_json() for item in items]

    @classmethod
    def add_(cls, data):
        obj = cls.new(data)
        obj.add_one()
        return obj

    @classmethod
    def add(cls, data, sync=True):
        if hasattr(cls, '__schema__'):
            validate(instance=data, schema=cls.__schema__)
        obj = cls.add_(data)
        if sync:
            cls.commit()
        return obj

    @classmethod
    def save(cls, primary_key, data):
        if hasattr(cls, '__schema__'):
            validate(instance=data, schema=cls.__schema__)
        obj = cls.get(primary_key)
        obj.update(data)
        cls.commit()

    @classmethod
    def get(cls, primary_key):
        return cls.query.get(primary_key)

    @classmethod
    def delete(cls, sync=True, **kwargs):
        cls.make_query(**kwargs).delete(synchronize_session=False)
        if sync:
            cls.commit()

    @classmethod
    def make_flts(cls, **kwargs):
        return [getattr(cls, k) == v for k, v in kwargs.items() if v is not None]

    @classmethod
    def make_query(cls, **kwargs):
        flts = cls.make_flts(**kwargs)
        return cls.filter(*flts)

    @classmethod
    def get_list(cls, **kwargs):
        return cls.make_query(**kwargs).all()

    @classmethod
    def get_json(cls, primary_key):
        obj = cls.get(primary_key)
        return obj.to_json() if obj else {}

    @classmethod
    def get_jsons(cls, page_num=None, page_size=None, order_key=None, order_way='desc', **kwargs):
        if page_num or page_size:
            pagination = {
                'page_num': page_num if page_num else 1,
                'page_size': page_size if page_size else 20
            }
            query = cls.make_query(**kwargs)
            if order_key:
                query = query.order_by(getattr(cls, order_key).desc() if order_way == 'desc' else getattr(cls, order_key).asc())
            return cls.paginate(query, pagination)
        else:
            items = cls.get_list(**kwargs)
            return cls.jsonlize(items)

    @classmethod
    def get_attrs(cls, attr_names, **kwargs):
        flts = cls.make_flts(**kwargs)
        attrs = [getattr(cls, attr_name) for attr_name in attr_names]
        return cls.query_(*attrs).filter(*flts).all()

    @classmethod
    def get_map(cls, attr_names):
        rst_map = {}
        for item in cls.get_attrs(attr_names):
            a, b = item
            rst_map[a] = b
        return rst_map
