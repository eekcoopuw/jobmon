class ConnectionConfig(object):
    """
    The connection configuration for a jobmon instance, permanent or
    transitory. An immutable object.  It works in two distinct modes - by
    reading a file from the given directory, or by explicitly being passed the
    host and port.  The former is okay if the jobs and the central job monitor
    share a file system, but the latter must be used if they do not.

    Args
        host (string): in lieu of a filepath to the monitor info,
            you can specify the hostname and port directly
        port (int): in lieu of a filepath to the monitor info,
            you can specify the hostname and port directly
        request_retries (int, optional): How many times to attempt to contact
            the other side. Default=3
        request_timeout (int, optional): How many milliseconds to wait for a
            response from the other side. Default=10 seconds
    """

    def __init__(self, host, port, request_retries=3, request_timeout=30000):

        self.host = host
        self.port = port
        self.request_retries = request_retries
        self.request_timeout = request_timeout

    def __repr__(self):
        return \
            """ConnectionConfig(host=host, port=port,
               request_retries={}, request_timeout={})""".format(
                self.host, self.port, self.request_retries,
                self.request_timeout)
