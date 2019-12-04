import pytest

from jobmon.setup_config import SetupCfg
from jobmon.server.config import ServerConfig


# @pytest.fixture
# def envvars():
#
#     conn_vars = {
#         "DB_HOST": "somehost",
#         "DB_PORT": "123456789",
#         "DB_USER": "foo",
#         "DB_PASS": "bar",
#         "DB_NAME": "baz",
#     }
#
#     # Store the original variables to be restored at teardown and override
#     orig_vars = {}
#     for k, v in conn_vars.items():
#         orig_vars[k] = os.environ[k]
#         os.environ[k] = v
#
#     yield conn_vars
#
#     # Restore originals
#     for k, v in orig_vars.items():
#         os.environ[k] = v
#
# fixture only used by other functions that are commented out
# @pytest.fixture
# def clear_envvars():
#
#     vars_to_unset = ["DB_HOST", "DB_PORT", "DB_USER", "DB_PASS", "DB_NAME"]
#     # Store the original variables to be restored at teardown and unset
#     orig_vars = {}
#     for ev in vars_to_unset:
#         orig_vars[ev] = os.environ[ev]
#         os.unsetenv(ev)
#
#     yield
#
#     # Restore originals
#     for k, v in orig_vars.items():
#         os.environ[k] = v

# @pytest.fixture
# def envvars(monkeypatch):
#
#     conn_vars = {
#         "DB_HOST": "somehost",
#         "DB_PORT": "123456789",
#         "DB_USER": "foo",
#         "DB_PASS": "bar",
#         "DB_NAME": "baz",
#     }
#
#     # override env variable (monkeypatch will revert them at teardown)
#     for k, v in conn_vars.items():
#         monkeypatch.setenv(k, v)
#
#     yield conn_vars


# def test_env_override_of_conn_str(envvars):
#     def_cfg = ServerConfig.from_defaults()
#     exp_conn_str = "mysql://{u}:{p}@{h}:{po}/{d}".format(
#         u=envvars["DB_USER"],
#         p=envvars["DB_PASS"],
#         h=envvars["DB_HOST"],
#         d=envvars["DB_NAME"],
#         po=envvars["DB_PORT"],
#     )
#     assert def_cfg.conn_str == exp_conn_str
