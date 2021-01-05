
# Use as base class for server side error
class ServerSideException(Exception):
    """Used for all exceptions on the server side (JQS, JSM)"""

    def __init__(self, msg: str):
        self.msg = msg

    def __str__(self):
        return repr(self.msg)


# Use for error caused by client mistakes
class InvalidUsage(ServerSideException):
    # make status_code a parameter for future extension. So far we only use 400
    def __init__(self, msg, status_code=None, payload=None):
        super().__init__(msg)
        self.status_code = status_code
        if self.status_code is None:
            # by default, use 400
            self.status_code = 400
        self.payload = payload

    def to_dict(self):
        rv = dict(self.payload or ())
        rv['message'] = self.msg
        return rv


# Use for Internal Server Error
class ServerError(ServerSideException):
    # make status_code a parameter for future extension. So far we only use 500
    def __init__(self, msg, status_code=None, payload=None):
        super().__init__(msg)
        self.status_code = status_code
        if self.status_code is None:
            # by default, use 500
            self.status_code = 500
        self.payload = payload

    def to_dict(self):
        rv = dict(self.payload or ())
        rv['message'] = self.msg
        return rv
