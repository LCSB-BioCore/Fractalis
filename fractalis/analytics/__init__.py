from fractalis.utils import list_classes_with_base_class
from .job import AnalyticsJob


JOB_REGISTRY = list_classes_with_base_class('fractalis.analytics.jobs',
                                            AnalyticsJob)
