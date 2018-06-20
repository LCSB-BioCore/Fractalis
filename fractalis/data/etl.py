"""This module provides the ETL class"""

import abc
import json
import logging
import os

from Cryptodome.Cipher import AES
# noinspection PyProtectedMember
from celery import Task
from pandas import DataFrame

from fractalis import app, redis
from fractalis.data.check import IntegrityCheck
from fractalis.utils import get_cache_encrypt_key

logger = logging.getLogger(__name__)


class ETL(Task, metaclass=abc.ABCMeta):
    """This is an abstract class that implements a celery Task and provides a
    factory method to create instances of implementations of itself. Its main
    purpose is to  manage extraction of the data from the target server. ETL
    stands for (E)xtract (T)ransform (L)oad and not by coincidence similar
    named methods can be found in this class.
    """

    @property
    @abc.abstractmethod
    def name(self) -> str:
        """Used by celery to identify this task by name."""
        pass

    @property
    @abc.abstractmethod
    def produces(self) -> str:
        """This specifies the fractalis internal format that this ETL
        produces. Can be one of: ['categorical', 'numerical', 'numerical_array']
        """
        pass

    @staticmethod
    @abc.abstractmethod
    def can_handle(handler: str, descriptor: dict) -> bool:
        """Check if the current implementation of ETL can handle given handler
        and data type.
        WARNING: You should never raise an Exception here and expect it to be
        propagated further up. It will be caught and assumed that the
        current ETL cannot handle the given arguments.
        :param handler: Describes the handler. E.g.: transmart, ada
        :param descriptor: Describes the data that we want to download.
        :return: True if implementation can handle given parameters.
        """
        pass

    @staticmethod
    def factory(handler: str, descriptor: dict) -> 'ETL':
        """Return an instance of the implementation ETL that can handle the
        given parameters.
        :param handler: Describes the handler. E.g.: transmart, ada
        :param descriptor: Describes the data that we want to download.
        :return: An instance of an implementation of ETL that returns True for
        can_handle()
        """
        from . import ETL_REGISTRY
        for ETLTask in ETL_REGISTRY:
            # noinspection PyBroadException
            try:
                if ETLTask.can_handle(handler, descriptor):
                    return ETLTask()
            except Exception as e:
                logger.warning("Caught exception and assumed that ETL '{}' "
                               "cannot handle handler '{}' "
                               "and descriptor: '{}'. Exception:'{}'".format(
                                   type(ETLTask).__name__,
                                   handler,
                                   str(descriptor), e))
                continue

        raise NotImplementedError(
            "No ETL implementation found for handler '{}' "
            "and descriptor '{}'".format(handler, descriptor))

    @abc.abstractmethod
    def extract(self, server: str, token: str, descriptor: dict) -> object:
        """Extract the data via HTTP requests.
        :param server: The server from which to extract from.
        :param token: The token used for authentication.
        :param descriptor: The descriptor containing all necessary information
        to download the data.
        """
        pass

    @abc.abstractmethod
    def transform(self, raw_data: object, descriptor: dict) -> DataFrame:
        """Transform the data into a pandas.DataFrame with a naming according to
        the Fractalis standard format.
        :param raw_data: The return value of extract().
        :param descriptor: The data descriptor, sometimes needed
        for transformation
        """
        pass

    def sanity_check(self):
        """Check whether ETL is still sane and should be continued. E.g. if
        redis has been cleared it does not make sense to proceed. Raise an
        exception if not sane."""
        check_1 = redis.exists('data:{}'.format(self.request.id))
        if not check_1:
            error = "ETL failed! The associated entry in " \
                    "Redis has been removed while the ETL was running."
            logger.error(error)
            raise RuntimeError(error)

    def update_redis(self, data_frame: DataFrame) -> None:
        """Set several meta information that can be used to filter the data
        before the analysis.
        :param data_frame: The extracted and transformed data.
        """
        value = redis.get(name='data:{}'.format(self.request.id))
        assert value is not None
        data_state = json.loads(value)
        if 'feature' in data_frame.columns:
            features = data_frame['feature'].unique().tolist()
        else:
            features = []
        data_state['meta']['features'] = features
        redis.setex(name='data:{}'.format(self.request.id),
                    value=json.dumps(data_state),
                    time=app.config['FRACTALIS_DATA_LIFETIME'])

    @staticmethod
    def secure_load(data_frame: DataFrame, file_path: str) -> None:
        """Save data to the file system in encrypted form using AES and the
        web service secret key. This can be useful to comply with certain
        security standards.
        :param data_frame: DataFrame to write.
        :param file_path: File to write to.
        """
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        data = data_frame.to_json()
        data = data.encode('utf-8')
        key = get_cache_encrypt_key(app.config['SECRET_KEY'])
        cipher = AES.new(key, AES.MODE_EAX)
        ciphertext, tag = cipher.encrypt_and_digest(data)
        with open(file_path, 'wb') as f:
            [f.write(x) for x in (cipher.nonce, tag, ciphertext)]

    @staticmethod
    def load(data_frame: DataFrame, file_path: str) -> None:
        """Load (save) the data to the file system.
        :param data_frame: DataFrame to write.
        :param file_path: File to write to.
        """
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        data_frame.to_pickle(file_path, compression='gzip')

    def run(self, server: str, token: str,
            descriptor: dict, file_path: str,
            encrypt: bool) -> None:
        """Run extract, transform and load. This is called by the celery worker.
        This is called by the celery worker.
        :param
        :param server: The server on which the data are located.
        :param token: The token used for authentication.
        :param descriptor: Contains all necessary information to download data
        :param file_path: The location where the data will be stored
        :param encrypt: Whether or not the data should be encrypted.
        :return: The data id. Used to access the associated redis entry later
        """
        logger.info("Starting ETL process ...")
        logger.info("(E)xtracting data from server '{}'.".format(server))
        try:
            self.sanity_check()
            raw_data = self.extract(server, token, descriptor)
        except Exception as e:
            logger.exception(e)
            raise RuntimeError("Data extraction failed. {}".format(e))
        logger.info("(T)ransforming data to Fractalis format.")
        try:
            self.sanity_check()
            data_frame = self.transform(raw_data, descriptor)
            checker = IntegrityCheck.factory(self.produces)
            checker.check(data_frame)
        except Exception as e:
            logger.exception(e)
            raise RuntimeError("Data transformation failed. {}".format(e))
        if not isinstance(data_frame, DataFrame):
            error = "transform() must return 'pandas.DataFrame', " \
                    "but returned '{}' instead.".format(type(data_frame))
            logging.error(error, exc_info=1)
            raise TypeError(error)
        try:
            self.sanity_check()
            if encrypt:
                self.secure_load(data_frame, file_path)
            else:
                self.load(data_frame, file_path)
            self.update_redis(data_frame)
        except Exception as e:
            logger.exception(e)
            raise RuntimeError("Data loading failed. {}".format(e))
