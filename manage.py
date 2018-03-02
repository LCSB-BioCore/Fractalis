from flask_script import Manager

import fractalis.cleanup
from fractalis import app


manager = Manager(app)


@manager.command
def janitor():
    fractalis.cleanup.janitor.delay()


if __name__ == "__main__":
    manager.run()
