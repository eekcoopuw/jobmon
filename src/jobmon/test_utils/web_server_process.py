"""A context manager to handle the creation/teardown of the web service for testing."""
import multiprocessing as mp
import os
import signal
import socket
import sys
from time import sleep
from types import TracebackType
from typing import Any, Optional

import requests


class WebServerProcess:
    """Context manager creates the Jobmon web server in a process and tears it down on exit."""

    def __init__(self, ephemera: dict) -> None:
        """Initializes the web server process.

        Args:
            ephemera: a dictionary containing the connection information for the database,
            specifically the database host, port, service account user, service account
            password, and database name
        """
        self.ephemera = ephemera
        if sys.platform == "darwin":
            self.web_host = ephemera["DB_HOST"]
        else:
            self.web_host = socket.getfqdn()
        self.web_port = str(10_000 + os.getpid() % 30_000)

    def __enter__(self) -> Any:
        """Starts the web service process."""
        # jobmon_cli string
        argstr = (
            'web_service test '
            f'--db_host {self.ephemera["DB_HOST"]} '
            f'--db_port {self.ephemera["DB_PORT"]} '
            f'--db_user {self.ephemera["DB_USER"]} '
            f'--db_pass {self.ephemera["DB_PASS"]} '
            f'--db_name {self.ephemera["DB_NAME"]} '
            f'--web_service_port {self.web_port}')

        def run_server_with_handler(argstr: str) -> None:
            def sigterm_handler(_signo: signal, _stack_frame: Any) -> None:
                # catch SIGTERM and shut down with 0 so pycov finalizers are run
                # Raises SystemExit(0):
                sys.exit(0)
            from jobmon.server.cli import main

            signal.signal(signal.SIGTERM, sigterm_handler)
            main(argstr)

        ctx = mp.get_context('fork')
        self.p1 = ctx.Process(target=run_server_with_handler, args=(argstr,))
        self.p1.start()

        # Wait for it to be up
        status = 404
        count = 0
        max_tries = 60
        while not status == 200 and count < max_tries:
            try:
                count += 1
                r = requests.get(f'http://{self.web_host}:{self.web_port}/health')
                status = r.status_code
            except Exception:
                # Connection failures land here
                # Safe to catch all because there is a max retry
                pass
            # sleep outside of try block!
            sleep(3)

        if count >= max_tries:
            raise TimeoutError(
                f"Out-of-process jobmon services did not answer after "
                f"{count} attempts, probably failed to start.")

        return self

    def __exit__(self, exc_type: Optional[BaseException], exc_value: Optional[BaseException],
                 exc_traceback: Optional[TracebackType]) -> None:
        """Terminate the web service process."""
        # interrupt and join for coverage
        self.p1.terminate()
        self.p1.join()
