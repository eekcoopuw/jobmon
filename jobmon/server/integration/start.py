import logging
import sys

import jobmon.server.integration.qpid.qpid_integrator as qpid


def start_qpid_integration():
    """Start the qpid integration service"""
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)

    qpid.maxpss_forever()
