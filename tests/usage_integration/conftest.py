import json
import os

import pytest


@pytest.fixture(scope='module')
def usage_integrator_config(ephemera):
    """This creates a usage integrator config.

    Only created if a test with the usage_integrator marker is selected.
    """

    from jobmon.server.usage_integration.config import UsageConfig

    json_path = os.path.join(os.path.dirname(__file__),
                             'integrator_secrets.json')

    # Read in the local integrator_secrets.json file, or create it if it doesn't exist
    try:
        with open(json_path, 'r') as f:
            integrator_config_dict = json.loads(f.read())
    except (json.decoder.JSONDecodeError, FileNotFoundError):
        # Missing or improperly formatted json. Pull from environment variables. If not found
        # default to random values, since this fixture shouldn't be used anyways.
        integrator_config_dict = {
            "DB_HOST_SLURM_SDB": os.getenv("DB_HOST_SLURM_SDB", "not.a.host"),
            "DB_PASS_SLURM_SDB": os.getenv("DB_PASS_SLURM_SDB", "not_a_pass"),
            "DB_USER_SLURM_SDB": os.getenv("DB_USER_SLURM_SDB", "not_a_user"),
            "DB_NAME_SLURM_SDB": os.getenv("DB_NAME_SLURM_SDB", "not_a_name"),
            "DB_PORT_SLURM_SDB": os.getenv("DB_PORT_SLURM_SDB", "3306")
        }

    # Combine with ephemera to create the usage integrator config
    integrator_config = UsageConfig(
        db_host=ephemera["DB_HOST"],
        db_user=ephemera["DB_USER"],
        db_pass=ephemera["DB_PASS"],
        db_name=ephemera["DB_NAME"],
        db_port=ephemera["DB_PORT"],
        db_host_slurm_sdb=integrator_config_dict["DB_HOST_SLURM_SDB"],
        db_user_slurm_sdb=integrator_config_dict["DB_USER_SLURM_SDB"],
        db_pass_slurm_sdb=integrator_config_dict["DB_PASS_SLURM_SDB"],
        db_name_slurm_sdb=integrator_config_dict["DB_NAME_SLURM_SDB"],
        db_port_slurm_sdb=integrator_config_dict["DB_PORT_SLURM_SDB"],
        slurm_polling_interval=10,
        slurm_max_update_per_second=100,
        slurm_cluster='slurm',
    )
    return integrator_config


@pytest.fixture
def usage_integrator(ephemera, usage_integrator_config):
    """Creates a configured instance of the usage integrator."""

    from jobmon.server.usage_integration.usage_integrator import UsageIntegrator

    # Create the usage integrator, and yield. On teardown close the connections
    integrator = UsageIntegrator(usage_integrator_config)
    yield integrator

    integrator.session.close()
    integrator.session_slurm_sdb.close()
