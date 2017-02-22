from fractalis.utils import list_classes_with_base_class
from fractalis.data.etls.etlhandler import ETLHandler
from fractalis.data.etls.etl import ETL

HANDLER_REGISTRY = list_classes_with_base_class('fractalis.data.etls',
                                                ETLHandler)
ETL_REGISTRY = list_classes_with_base_class('fractalis.data.etls',
                                            ETL)
