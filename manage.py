import os
import json

from flask_script import Manager

from fractalis import app, redis
from fractalis.sync import remove_file


manager = Manager(app)


@manager.command
def janitor() -> None:
    """Ideally this is maintained by a systemd service to cleanup redis and the
    file system while Fractalis is running.
    """
    raise NotImplementedError()


if __name__ == "__main__":
    manager.run()
