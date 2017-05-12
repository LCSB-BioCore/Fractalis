from fractalis.utils import list_classes_with_base_class
from fractalis.analytics.task import AnalyticTask

TASK_REGISTRY = list_classes_with_base_class('fractalis.analytics.tasks',
                                             AnalyticTask)
