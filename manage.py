from flask_script import Manager

from fractalis import app, redis


manager = Manager(app)


@manager.command
def janitor():
    pubsub = redis.pubsub()
    pubsub.subscribe(['expired'])
    for msg in pubsub.listen():
        print(msg)

if __name__ == "__main__":
    manager.run()
