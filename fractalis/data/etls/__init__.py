"""This module helps us to populate __subclasses__() of ETLHandler and ETL"""
import os

current_path = os.path.dirname(os.path.abspath(__file__))
for dir_path, dir_names, file_names in os.walk(current_path):
    if (dir_path == current_path or
            '__pycache__' in dir_path or
            '__init__.py' not in file_names):
        continue
    dirname = os.path.basename(dir_path)
    exec('from fractalis.data.etls.{} import *'.format(dirname))
