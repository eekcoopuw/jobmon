"""Start the qpid integration service."""
import logging
import sys

import jobmon.server.usage_integration.usage_integrator as usage_integrator


def start_usage_integration() -> None:
    """Start the qpid integration service."""
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)

    usage_integrator.q_forever()
