import os
import subprocess
import time
import warnings
import logging
from jobmon.sender import Sender
import jobmon.sge as sge


__mod_name__ = "jobmon"
logger = logging.getLogger(__mod_name__)


class CentralJobMonitorRunning(Exception):
    pass


class CentralJobMonitorStartLocked(Exception):
    pass


class CentralJobMonitorLauncher:
    """Runs on the central node. Is only responsible for starting/stopping the
    CentralJobMonitor.  This is NOT the CentralJobMonitor"""

    def __init__(self, out_dir, request_retries, request_timeout):
        logger.debug("CentralJobMonitorLauncher created in dir '{}', retries {}, timeout {}".format(out_dir,
                                                                                                    request_retries,
                                                                                                    request_timeout))
        self.sender = Sender(out_dir, request_retries, request_timeout)
        self.lock_file_path = self.sender.out_dir + "/start.lock"
        # kwargs expected request_retries=3, request_timeout=3000

    def stop_server(self):
        """stop a running server"""
        logger.info("Attempting to stop the CentralJobMonitor")
        response = self.sender.send_request('stop')
        print(response[1])

    def is_alive(self):
        """check whether CentralJobMonitor is alive.

        Returns:
            boolean True of False if it is alive."""

        # will return false here if no monitor_info.json exists
        try:
            # Causes too many connections, use a ping instead
            if not self.sender.is_connected():
                self.sender.connect()
        except IOError:
            return False

        # next we ping to see if server is alive under the monitor_info.json
        r = self.sender.send_request({"action": "alive", "args": ""})
        if isinstance(r, tuple):
            if r[0] == 0:
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
        logger.debug("Launcher attempting to start the server, restart is {}, nolock is {}".format(restart, nolock))
        # check if there is already a server here
        if self.is_alive():
            if not restart:
                logger.debug("Server is already alive, no action taken")
                raise CentralJobMonitorRunning("Server is already alive, no action taken")
            else:
                logger.info("Server is already alive. will stop previous server and restart.")
                self.stop_server()

        # check if there is a start lock in the file system.

        logger.debug("Launcher checking start lock file at {}".format(self.lock_file_path))
        if os.path.isfile(self.lock_file_path):
            if not nolock:
                logger.debug(" lock file is present, and locking is enforced. Raising exception")
                raise CentralJobMonitorStartLocked(
                    "Server is already starting. If this is not the case "
                    "either remove 'start.lock' from server directory or use "
                    "option 'force'")
            else:
                warnings.warn("bypassing startlock. not recommended!!!!")
        else:
            logger.debug("    lock file not present")

        # Pop open a new server instance on current node.

        try:
            self._create_lock_file()
            shell = sge.true_path(executable="env_submit_master.sh")
            prepend_to_path = sge.true_path(file_or_dir=path_to_conda_bin_on_target_vm)

            # launch_central_monitor.py creates CentralJobMonitor
            logger.debug("Calling popen, my pid is {}".format(os.getpid()))
            pop = subprocess.Popen([shell, prepend_to_path, conda_env,
                                    "launch_central_monitor.py", self.sender.out_dir])
            logger.debug("Sleeping to allow server to start, child process id is {}".format(pop.pid))
            # TODO  replace by busy-wait loop
            time.sleep(45)
            self._remove_lock_file()

            # check if it booted properly
            if self.is_alive():
                logger.info("{}: Successfully started CentralJobMonitor".format(os.getpid()))
                return True
            else:
                logger.info("{}: Server failed to start CentralJobMonitor".format(os.getpid()))
                return False
        except Exception as e:
            warnings.warn("Could not start the server: '{}', removing start lock at {}".format(e, self.lock_file_path))

    def _create_lock_file(self):
        logger.debug("Creating start lock file {}".format(self.lock_file_path))
        open(self.lock_file_path, 'w').close()

    def _remove_lock_file(self):
        logger.debug("Removing start lock file {}".format(self.lock_file_path))
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
