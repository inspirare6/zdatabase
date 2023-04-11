from zdatabase import session


class DatabaseUtility:
    @staticmethod
    def flush():
        session.flush()

    @staticmethod
    def commit():
        session.commit()

    @staticmethod
    def query_(*args, **kwargs):
        return session.query(*args, **kwargs)

    @staticmethod
    def add_all(items):
        session.add_all(items)
        session.commit()
        return items


class QueryUtility:
    @classmethod
    def select(cls, params, conds):
        """ 筛选(模糊匹配）
        ?name=1&asset_sn=2019-BG-5453
        """
        flts = []
        for cond in conds:
            value = params.get(cond)
            if value:
                flts.append(getattr(cls, cond).like(f'%{value}%'))
        return flts
    
    @classmethod
    def select_(cls, params, conds):
        """ 筛选(精确匹配）
        ?name=1&asset_sn=2019-BG-5453
        """
        flts = []
        for cond in conds:
            value = params.get(cond)
            if value:
                flts.append(getattr(cls, cond) == value)
        return flts

    @classmethod
    def select_date(cls, attr_name, params):
        """ 日期筛选"""
        flts = []
        start_date = params.get('start_date')
        end_date = params.get('end_date')
        if start_date:
            flts.append(getattr(cls, attr_name) >= start_date)
        if end_date:
            flts.append(getattr(cls, attr_name) <= end_date)
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
        page_num = params.get('page_num')
        page_size = params.get('page_size')
        page_num = int(page_num) if page_num else 1
        page_size = int(page_size) if page_size else 10
        offset_num = (page_num - 1) * page_size
        items = query.offset(offset_num).limit(page_size).all()
        total = query.count()
        rst = {
            'items': [getattr(item, method)() for item in items],
            'total': total
        }
        return rst

    @staticmethod
    def paginate2(cls, query, params, method):
        """分页
        page_size=100&page_num=1
        """
        page_num = params.get('page_num')
        page_size = params.get('page_size')
        page_num = int(page_num) if page_num else 1
        page_size = int(page_size) if page_size else 10
        offset_num = (page_num - 1) * page_size
        rst = query.offset(offset_num).limit(page_size).all()
        total = query.count()
        rst = {
            'items': [method(item) for item in rst],
            'total': total
        }
        return rst


class MapperUtility:
    @staticmethod
    def jsonlize(items):
        return [item.to_json() for item in items]
    
    @classmethod
    def make_flts(cls, **kwargs):
        flts = []
        for k, v in kwargs.items():
            flts += [getattr(cls, k) == v]
        return flts

    @classmethod
    def make_query(cls, **kwargs):
        flts = cls.make_flts(**kwargs)
        return cls.filter(*flts)

    @classmethod
    def add_(cls, data):
        obj = cls.new(data)
        obj.add_one()
        return obj

    @classmethod
    def add(cls, data):
        obj = cls.add_(data)
        cls.commit()
        return obj

    @classmethod
    def add_list_(cls, items):
        for item in items:
            cls.add_(item)
        cls.commit()

    @classmethod
    def save_(cls, primary_key, data):
        obj = cls.get_(primary_key)
        if obj:
            obj.update(data)
        else:
            cls.add_(data)
        cls.commit()

    @classmethod
    def update_(cls, primary_key, data):
        obj = cls.get_(primary_key)
        obj.update(data)
        cls.commit()

    @classmethod
    def get_(cls, primary_key):
        return cls.query.get(primary_key)

    @classmethod
    def get_json(cls, primary_key):
        obj = cls.get_(primary_key)
        return obj.to_json() if obj else {}

    @classmethod
    def get_list_(cls, **kwargs):
        return cls.make_query(**kwargs).all()

    @classmethod
    def get_list(cls, **kwargs):
        items = cls.get_list_(**kwargs)
        return cls.jsonlize(items)

    @classmethod
    def get_page(cls, **kwargs):
        pagination = {
            'page_size': kwargs.pop('page_size', 20),
            'page_num': kwargs.pop('page_num', 1)
        }
        query = cls.make_query(**kwargs)
        return cls.paginate(query, pagination)

    @classmethod
    def get_all_(cls):
        return cls.filter().all()

    @classmethod
    def get_all(cls):
        items = cls.get_all_()
        return cls.jsonlize(items)

    @classmethod
    def get_attrs_(cls, attr_names, **kwargs):
        flts = cls.make_flts(**kwargs)
        attrs = [getattr(cls, attr_name) for attr_name in attr_names]
        return cls.query_(*attrs).filter(*flts).all()

    @classmethod
    def get_map_(cls, attr_names):
        rst_map = {}
        for item in cls.get_attrs_(attr_names):
            a, b = item
            rst_map[a] = b
        return rst_map

    @classmethod
    def delete_list_(cls, **kwargs):
        cls.make_query(**kwargs).delete(synchronize_session=False)
        cls.commit()
