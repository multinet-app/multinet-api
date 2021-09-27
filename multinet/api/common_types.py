from enum import Enum

class ColumnTypeEnum(Enum):
    LABEL = 'label'
    BOOLEAN = 'boolean'
    CATEGORY = 'category'
    NUMBER = 'number'
    DATE = 'date'

    @classmethod
    def values(cls):
        return list(map(lambda c: c.value, cls))