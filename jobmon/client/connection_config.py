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
    """

    def __init__(self, host, port):

        self.host = host
        self.port = port
        self.url = "http://{h}:{p}".format(h=host, p=port)

    def __repr__(self):
        return ("ConnectionConfig("
                "host={}, port={}, url={})".format(
                    self.host, self.port, self.url))
