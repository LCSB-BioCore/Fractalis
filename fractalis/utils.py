import os
import glob
import inspect
import importlib


def import_module_by_abs_path(module_path):
    module_name = os.path.splitext(os.path.basename(module_path))[0]
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def list_classes_with_base_class(package, base_class):
    package = importlib.import_module(package)
    abs_path = os.path.dirname(os.path.abspath(package.__file__))
    class_list = []
    for module_path in glob.iglob('{}/*/**/*.py'.format(abs_path),
                                  recursive=True):
        if not os.path.basename(module_path).startswith('_'):
            module = import_module_by_abs_path(module_path)
            classes = inspect.getmembers(module, inspect.isclass)
            for name, obj in classes:
                if obj.__base__ == base_class:
                    class_list.append(obj)
    return class_list
