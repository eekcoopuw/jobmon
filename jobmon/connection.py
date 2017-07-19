

class CentralMonitorConnection(object):
    """The connection configuration of a single, permanent jobmon instance.
    Args
        monitor_host (string): in lieu of a filepath to the monitor info,
            you can specify the hostname and port directly
        monitor_port (int): in lieu of a filepath to the monitor info,
            you can specify the hostname and port directly
        request_retries (int, optional): How many times to attempt to contact
            the central job monitor. Default=3
        request_timeout (int, optional): How many milliseconds to wait for a response from
            the central job monitor. Default=10 seconds
    """


    def __init__(self, monitor_host, monitor_port,
                 request_retries=3, request_timeout=10000):
        self.monitor_host = monitor_host
        self.monitor_port = monitor_port
        self.request_retries = request_retries
        self.request_timeout = request_timeout