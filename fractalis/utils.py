import os
import inspect
import importlib
from pathlib import Path
from typing import List
import importlib.util


def import_module_by_abs_path(module_path: str) -> object:
    """Import the module for the given path.
    :param module_path: The absolute path to the module.
    :return: A reference to the imported module.
    """
    module_name = os.path.splitext(os.path.basename(module_path))[0]
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def list_classes_with_base_class(
        package: str, base_class: object) -> List[object]:
    """For the given base_class list classes in the given package that do
    implement it.
    :param package: The package to search.
    :param base_class: The base class.
    :return: A list of classes that implement the base class.
    """
    package = importlib.import_module(package)
    abs_path = os.path.dirname(os.path.abspath(package.__file__))
    class_list = []
    for f in Path(abs_path).glob('*/**/*.py'):
        module_path = str(f)
        if not os.path.basename(module_path).startswith('_'):
            module = import_module_by_abs_path(module_path)
            classes = inspect.getmembers(module, inspect.isclass)
            for name, obj in classes:
                if obj.__base__ == base_class:
                    class_list.append(obj)
    return class_list


def get_cache_encrypt_key(key):
    """Prepare key for use with crypto libs.
    :param key: Passphrase used for encryption.
    """
    key += (16 - (len(key) % 16)) * '-'
    return key.encode('utf-8')
