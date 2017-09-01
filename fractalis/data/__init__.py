from fractalis.utils import list_classes_with_base_class
from .etlhandler import ETLHandler
from .etl import ETL
from .check import IntegrityCheck

HANDLER_REGISTRY = list_classes_with_base_class('fractalis.data.etls',
                                                ETLHandler)
ETL_REGISTRY = list_classes_with_base_class('fractalis.data.etls',
                                            ETL)
CHECK_REGISTRY = list_classes_with_base_class('fractalis.data',
                                              IntegrityCheck)
