import os
import subprocess
import time
import warnings
import logging
from jobmon.sender import Sender
import jobmon.sge as sge


class CentralJobMonitorRunning(Exception):
    pass


class CentralJobMonitorStartLocked(Exception):
    pass


class CentralJobMonitorLauncher:
    """Runs on the central node. Is only responsible for starting/stopping the
    CentralJobMonitor.  This is NOT the CentralJobMonitor"""

    def __init__(self, out_dir, request_retries, request_timeout):
        self.logger = logging.getLogger(__name__)
        self.logger.debug("CentralJobMonitorLauncher created in dir '{}', retries {}, timeout {}".format(out_dir,
                                                                                                         request_retries,
                                                                                                         request_timeout))
        self.sender = Sender(out_dir, request_retries, request_timeout)
        self.lock_file_path = self.sender.out_dir + "/start.lock"
        # kwargs expected request_retries=3, request_timeout=3000
        self.max_boot_time = 45

    def stop_server(self):
        """stop a running server"""
        self.logger.info("{}: Attempting to stop the CentralJobMonitor".format(os.getpid()))
        response = self.sender.send_request('stop')
        print(response[1])

    def is_alive(self):
        """check whether CentralJobMonitor is alive.

        Returns:
            boolean True of False if it is alive."""

        # will return false here if no monitor_info.json exists
        try:
            # Always connecting uses too many connections, use a ping instead
            if not self.sender.is_connected():
                self.sender.connect()
        except IOError:
            return False

        # next we ping to see if server is alive under the monitor_info.json
        r = self.sender.send_request({"action": "alive", "args": ""})
        if isinstance(r, tuple):
            if r[0] == 0:
                self.logger.info("{}: CentralJobMonitor is alive, its pid is {}".format(os.getpid(), r[1]))
                return True
            else:
                return False
        else:
            return False

    def start_server(self, path_to_conda_bin_on_target_vm, conda_env, restart=False,
                     nolock=False):
        """Start new server instance

        Args:
            path_to_conda_bin_on_target_vm (string): anaconda bin to prepend to path
            conda_env (string): python >= 3.5 conda env to run server in.
            restart (bool, optional): whether to force a new server instance to
                start. Will shutdown existing server instance if one exists.
            nolock (bool, optional): ignore any boot locks for the specified
                directory. Highly not recommended.

        Returns:
            Boolean whether the server started successfully or not.
        """
        self.logger.debug("Launcher attempting to start the server, restart is {}, nolock is {}".format(restart, nolock))
        # check if there is already a server here
        if self.is_alive():
            if not restart:
                self.logger.debug("Server is already alive, no action taken")
                raise CentralJobMonitorRunning("Server is already alive, no action taken")
            else:
                self.logger.info("Server is already alive. will stop previous server and restart.")
                self.stop_server()

        # check if there is a start lock in the file system.
        self.logger.debug("Launcher checking start lock file at {}".format(self.lock_file_path))
        if os.path.isfile(self.lock_file_path):
            if not nolock:
                self.logger.info("Lock file is present at {}, and locking is enforced. Raising exception".format(
                    self.lock_file_path))
                raise CentralJobMonitorStartLocked(
                    "Server is already starting. If this is not the case "
                    "either remove 'start.lock' from server directory or use "
                    "option 'force'")
            else:
                warnings.warn("bypassing startlock. not recommended!!!!")
        else:
            self.logger.debug("    lock file not present")

        # Pop open a new server instance on current node.

        try:
            self._create_lock_file()
            shell = sge.true_path(executable="env_submit_master.sh")
            prepend_to_path = sge.true_path(file_or_dir=path_to_conda_bin_on_target_vm)

            # launch_central_monitor.py creates CentralJobMonitor
            self.logger.debug("{}: Calling popen".format(os.getpid()))
            pop = subprocess.Popen([shell, prepend_to_path, conda_env,
                                    "launch_central_monitor.py", self.sender.out_dir])
            self.logger.debug(
                "{}: Sleeping to allow server to start, child process id is {}".format(os.getpid(), pop.pid))

            # Use a sleep-wait loop
            boot_time_so_far = 0
            while not self.is_alive():
                if boot_time_so_far > self.max_boot_time:
                    self.logger.info("{}: Server failed to start CentralJobMonitor".format(os.getpid()))
                    self._remove_lock_file()
                    return False
                time.sleep(5)
                boot_time_so_far += 5

            # The server is alive
            self._remove_lock_file()
            self.logger.info("{}: Successfully started CentralJobMonitor".format(os.getpid()))

        except Exception as e:
            warnings.warn("Could not start the server: '{}', removing start lock at {}".format(e, self.lock_file_path))

    def _create_lock_file(self):
        self.logger.debug("Creating start lock file {}".format(self.lock_file_path))
        open(self.lock_file_path, 'w').close()

    def _remove_lock_file(self):
        self.logger.debug("Removing start lock file {}".format(self.lock_file_path))
        os.remove(self.lock_file_path)

    def query(self, query):
        """execute query on sqlite database through server
        Args:
            query (string): raw sql query string to execute on sqlite monitor
                database
        """
        msg = {'action': 'query', 'args': [query]}
        response = self.sender.send_request(msg)
        return response
