"""Start the qpid integration service."""
import logging
import sys

import jobmon.server.squid_integration.squid_integrator as squid


def start_qpid_integration() -> None:
    """Start the qpid integration service."""
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)

    squid.maxrss_forever()
