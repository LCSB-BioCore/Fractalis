from fractalis.celery import get_scripts_packages

packages = get_scripts_packages()
for package in packages:
    exec('import {}.tasks'.format(package))
