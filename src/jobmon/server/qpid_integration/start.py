"""Start the qpid integration service."""
import logging
import sys

import jobmon.server.qpid_integration.qpid_integrator as qpid


def start_qpid_integration() -> None:
    """Start the qpid integration service."""
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)

    qpid.maxpss_forever()
