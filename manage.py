import os
import json

from flask_script import Manager

from fractalis import app, redis
from fractalis.sync import remove_file


manager = Manager(app)


@manager.command
def janitor():
    pubsub = redis.pubsub()
    pubsub.psubscribe('*')
    for msg in pubsub.listen():
        if msg['data'] == 'expired':
            print(msg)
            split = msg['channel'].split(':')
            if 'shadow' == split[1] and 'data' == split[2]:
                expired_key = ':'.join(split[2:])
                expired_value = redis.get(expired_key)
                expired_value = json.loads(expired_value.decode('utf-8'))
                file_path = expired_value['file_path']
                print('deleting file: ', file_path)
                print('deleting redis key: ', expired_key)
                redis.delete(expired_key)
                remove_file.delay()


if __name__ == "__main__":
    manager.run()