import re


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
