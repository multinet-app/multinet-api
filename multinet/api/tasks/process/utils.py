from datetime import datetime
from enum import Enum
import json
from typing import Optional, Union

import celery
from celery.utils.log import get_task_logger
from dateutil import parser as dateutilparser

from multinet.api.models import Upload

logger = get_task_logger(__name__)


class ColumnTypeEnum(Enum):
    LABEL = 'label'
    BOOLEAN = 'boolean'
    CATEGORY = 'category'
    NUMBER = 'number'
    DATE = 'date'

    @classmethod
    def values(cls):
        return list(map(lambda c: c.value, cls))


class ProcessUploadTask(celery.Task):
    """
    A celery task for upload processing.

    NOTE: This task assumes that all arguments are passed using kwargs.
    If an argument is passed positionally, this task will fail.
    """

    @staticmethod
    def start_upload(upload_id: int):
        logger.info(f'Begin processing of upload {upload_id}')
        upload: Upload = Upload.objects.get(id=upload_id)
        upload.status = Upload.Status.STARTED
        upload.save()

    @staticmethod
    def fail_upload_with_message(upload: Upload, message: str):
        upload.status = Upload.Status.FAILED
        if upload.error_messages is None:
            upload.error_messages = [message]
        else:
            upload.error_messages.append(message)

        upload.save()

    @staticmethod
    def complete_upload(upload: Upload):
        upload.status = Upload.Status.FINISHED
        upload.save()

    def __call__(self, *args, **kwargs):
        """Wrap the inherited `__call__` method to set upload status."""
        self.start_upload(kwargs['upload_id'])
        return self.run(*args, **kwargs)

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        upload: Upload = Upload.objects.get(id=kwargs['upload_id'])
        self.fail_upload_with_message(upload, exc)

    def on_success(self, retval, task_id, args, kwargs):
        upload: Upload = Upload.objects.get(id=kwargs['upload_id'])
        self.complete_upload(upload)


def str_to_bool(entry: str) -> bool:
    """Try to determine base format of boolean so it can be converted properly."""

    def from_int(x: str) -> Optional[bool]:
        if x == '0' or x == '1':
            return bool(int(x))
        return None

    def from_json_bool(x: str) -> Optional[bool]:
        if x == 'true' or x == 'false':
            return json.loads(x)
        return None

    def from_yaml_bool(x: str) -> Optional[bool]:
        if x == 'no' or x == 'off':
            return False
        if x == 'yes' or x == 'on':
            return True

        return None

    def cast_col_entry(x: str) -> bool:
        casters = [from_int, from_json_bool, from_yaml_bool]
        for caster in casters:
            cast_row = caster(x)
            if cast_row is not None:
                return cast_row

        raise ValueError

    return cast_col_entry(entry)


def str_to_datestr(entry: str) -> str:
    """Try to read a date as an ISO 8601 string or unix timestamp."""
    try:
        # Raises ValueError is entry is not a float/int
        # Raises ValueError if the number is out of range
        return datetime.fromtimestamp(float(entry)).isoformat()
    except ValueError:
        # Raises ValueError if it cannot parse the string into a datetime object
        return dateutilparser.parse(entry).isoformat()


def str_to_number(entry: str) -> Union[int, float]:
    """Try to read a number from a given string."""
    try:
        return int(entry)
    except ValueError:
        return float(entry)


# Store mapping of enums to processor functions
processor_dict = {
    ColumnTypeEnum.BOOLEAN.value: str_to_bool,
    ColumnTypeEnum.DATE.value: str_to_datestr,
    ColumnTypeEnum.NUMBER.value: str_to_number,
}
