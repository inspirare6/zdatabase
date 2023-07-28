import arrow


def format(a, fmt='YYYY-MM-DD'):
    """ 日期格式化"""
    if not isinstance(a, arrow.arrow.Arrow):
        a = arrow.get(a) if a else None
    return a.format(fmt) if a else None

