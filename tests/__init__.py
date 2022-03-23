import getpass
import os
import logging
import time

import pytest
from unittest.mock import patch

from jobmon.client.tool import Tool
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from jobmon.server.usage_integration.usage_integrator import UsageIntegrator
from jobmon.server.usage_integration.usage_queue import UsageQ
from jobmon.server.usage_integration.usage_utils import QueuedTI

logger = logging.getLogger(__name__)


class TUsageIntegrator(UsageIntegrator):

    def __init__(self) -> None:
        self._connection_params = {}
        self._slurm_api = None
        self.heartbeat_time = 0
        self.token_refresh_time = 0
        self.token_lifespan = 86400  # TODO: make this configurable
        self.user = "svcscicompci"

        # Initialize config
        # self.config = _get_config()
        conn_slurm_sdb_str = "mysql://{user}:{pw}@{host}:{port}/{db}".format(
            user="jobmon",
            pw="",
            host="gen-slurm-sdb-s02.cluster.ihme.washington.edu",
            port="3306",
            db="slurm_acct_db",
        )

        # # Initialize sqlalchemy session
        # eng = create_engine(self.config["conn_str"], pool_recycle=200)
        # session = sessionmaker(bind=eng)
        # self.session = session()

        # Initialize sqlalchemy session for slurm_sdb
        eng_slurm_sdb = create_engine(conn_slurm_sdb_str, pool_recycle=200)
        session_slurm_sdb = sessionmaker(bind=eng_slurm_sdb)
        self.session_slurm_sdb = session_slurm_sdb()

        self.tres_types = self._get_tres_types()
        logger.info(f"tres types = {self.tres_types}")