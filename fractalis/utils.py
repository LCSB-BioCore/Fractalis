import os
import importlib


def get_sub_packages_for_package(package):
    module = importlib.import_module(package)
    abs_path = os.path.dirname(os.path.abspath(module.__file__))
    sub_packages = []
    for dir_path, dir_names, file_names in os.walk(abs_path):
        if (dir_path == abs_path or
                '__pycache__' in dir_path or
                '__init__.py' not in file_names):
            continue
        dirname = os.path.basename(dir_path)
        sub_package = '{}.{}'.format(package, dirname)
        sub_packages.append(sub_package)
    return sub_packages
