import re
from typing import Dict, List, Optional

from multinet.api.models.workspace import Workspace


class Re:
    def __init__(self, pattern):
        if isinstance(pattern, type(re.compile(''))):
            self.pattern = pattern
        else:
            self.pattern = re.compile(pattern)

    def __eq__(self, other):
        return self.pattern.fullmatch(str(other)) is not None

    def __str__(self):
        return self.pattern.pattern

    def __repr__(self):
        return repr(self.pattern.pattern)


INTEGER_ID_RE = Re(r'\d+')
TIMESTAMP_RE = Re(r'\d{4}-\d{2}-\d{2}T\d{2}\:\d{2}\:\d{2}\.\d{6}Z')
HTTP_URL_RE = Re(r'http[s]?\://[^/]+(/[^/]+)*[/]?(&.+)?')
UUID_RE = Re(r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}')

ARANGO_COLL = Re(r'[A-Za-z][-\w]{1,255}')
ARANGO_DOC_KEY = Re(r'[-\w:\.@()+,=;$!*\'%]{1,254}')
ARANGO_DOC_ID = Re(str(ARANGO_COLL) + r'\/' + str(ARANGO_DOC_KEY))
ARANGO_DOC_REV = Re(r'[-_\w]+')


def s3_file_field_re(filename: str):
    return Re(f'{UUID_RE}/{filename}')


def workspace_re(workspace: Workspace):
    return {
        'id': workspace.pk,
        'name': workspace.name,
        'created': TIMESTAMP_RE,
        'modified': TIMESTAMP_RE,
        'arango_db_name': workspace.arango_db_name,
        'public': workspace.public,
        'starred': workspace.starred,
    }


def dict_to_fuzzy_arango_doc(d: Dict, exclude: Optional[List[str]] = None):
    doc_fields = {
        '_id': ARANGO_DOC_ID,
        '_key': ARANGO_DOC_KEY,
        '_rev': ARANGO_DOC_REV,
    }

    if exclude is not None:
        for key in exclude:
            doc_fields.pop(key, None)

    return {
        **d,
        **doc_fields,
    }


def arango_doc_to_fuzzy_rev(d: Dict):
    return {
        **d,
        '_rev': ARANGO_DOC_REV,
    }
