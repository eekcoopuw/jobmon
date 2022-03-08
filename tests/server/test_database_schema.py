import getpass
import os
import time

import pytest
from unittest.mock import patch

from jobmon.client.tool import Tool
from jobmon.server.usage_integration.usage_integrator import UsageIntegrator
from jobmon.server.usage_integration.usage_queue import UsageQ
from jobmon.server.usage_integration.usage_utils import QueuedTI


def test_arg_name_collation(db_cfg, ephemera):
    """test both lowercase and uppercase have no conflict."""
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        result = DB.session.execute(
            """
            INSERT INTO arg(name)
            VALUES
	            ('r'),
	            ('R'),
	            ('test_case'),
	            ('TEST_CASE');
	        """
        )
        DB.session.commit()
        assert result.rowcount == 4
