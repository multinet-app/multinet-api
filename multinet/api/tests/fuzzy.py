import re
from typing import Dict


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


INTEGER_ID_RE = Re(r'\d')
TIMESTAMP_RE = Re(r'\d{4}-\d{2}-\d{2}T\d{2}\:\d{2}\:\d{2}\.\d{6}Z')
HTTP_URL_RE = Re(r'http[s]?\://[^/]+(/[^/]+)*[/]?(&.+)?')
UUID_RE = Re(r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}')

ARANGO_COLL = Re(r'[A-Za-z][-\w]{1,255}')
ARANGO_DOC_KEY = Re(r'[-\w:\.@()+,=;$!*\'%]{1,254}')
ARANGO_DOC_ID = Re(str(ARANGO_COLL) + r'\/' + str(ARANGO_DOC_KEY))
ARANGO_DOC_REV = Re(r'[-\w]+')


def dict_to_fuzzy_arango_doc(d: Dict):
    return {
        **d,
        '_id': ARANGO_DOC_ID,
        '_key': ARANGO_DOC_KEY,
        '_rev': ARANGO_DOC_REV,
    }
