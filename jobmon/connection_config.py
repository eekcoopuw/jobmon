import json
import os


class ConnectionConfig(object):
    """The connection configuration for a jobmon instance, permanent or transitory. An immutable object.
    It works in two distinct modes - by reading a file from the given directory,
    or by explicitly being passed the host and port.
    The former is okay if the jobs and the central job monitor share a file system, but the latter must be used if
    they do not.

    Args
        monitor_dir (string): file path where the server configuration is
            stored.
        monitor_filename (string): the filename wihtin the directory,
            if using the directory version
        monitor_host (string): in lieu of a filepath to the monitor info,
            you can specify the hostname and port directly
        monitor_port (int): in lieu of a filepath to the monitor info,
            you can specify the hostname and port directly
        request_retries (int, optional): How many times to attempt to contact
            the other side. Default=3
        request_timeout (int, optional): How many milliseconds to wait for a response from
            the other side. Default=10 seconds
    """

    def __init__(self, monitor_dir=None, monitor_filename="monitor_info.json", monitor_host=None, monitor_port=None,
                 request_retries=3, request_timeout=10000):
        
        if not (bool(monitor_dir) ^ bool(monitor_host and monitor_port)):
            raise ValueError("Either out_dir or the combination monitor_host+"
                             "monitor_port must be specified. Cannot specify "
                             "both monitor_dir and a host+port pair.")

        self.monitor_dir = monitor_dir
        self.monitor_filename = monitor_filename
        self.monitor_host = monitor_host
        self.monitor_port = monitor_port
        self.request_retries = request_retries
        self.request_timeout = request_timeout
        self.json = None

        # For safety, load the host & port fields immediately
        self.load_monitor_info()
        
    def load_monitor_info(self):
        """Return a json object with 'host' and 'port', either by reading the info file from disk, OR from stored values
        provide to the initialiser"""
        if self.json:
            return self.json

        if self.monitor_dir:
            full_dir = os.path.abspath(os.path.expanduser(self.monitor_dir))
            full_dir = os.path.realpath(full_dir)
            with open("{}/{}".format(full_dir, self.monitor_filename)) as f:
                self.json = json.load(f)
                self.monitor_host = self.json['host']
                self.monitor_port = self.json['port']
        else:
            self.json = {'host': self.monitor_host, 'port': self.monitor_port}

        return self.json

    def __repr__(self):
        return \
            """ConnectionConfig(monitor_dir={}, monitor_filename={}, monitor_host={}, monitor_port={}, 
                request_retries={}, request_timeout={})""".format(
                                                          self.monitor_dir,
                                                          self.monitor_filename,
                                                          self.monitor_host,
                                                          self.monitor_port,
                                                          self.request_retries,
                                                          self.request_timeout
                                                          )
