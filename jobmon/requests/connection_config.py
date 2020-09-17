from jobmon import config

class ConnectionConfig(object):
    """
    This is intended to be a singleton. Any other usage should be done with
    CAUTION.
    """

    @classmethod
    def from_defaults(cls):
        return cls(host=config.jobmon_server_sqdn,
                   port=config.jobmon_service_port)

    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port

    @property
    def url(self):
        return f"http://{self.host}:{self.port}"
