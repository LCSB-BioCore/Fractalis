import os
import json
import datetime

from fractalis.celery import app as celery
from fractalis import redis
from fractalis import app


@celery.task
def cleanup(cache_expr=None):
    cache_expr = (app.config['FRACTALIS_CACHE_EXP'] if cache_expr is None
                  else cache_expr)
    data = redis.hgetall(name='data')
    for key in data:
        data_obj = data[key].decode('utf-8')
        data_obj = json.loads(data_obj)
        last_access = datetime.datetime.fromtimestamp(data_obj['last_access'])
        now = datetime.datetime.now()
        delta = now - last_access
        if delta > cache_expr:
            os.remove(data_obj['file_path'])
            redis.hdel('data', key)