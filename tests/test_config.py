import pytest
import os


try:
    FileNotFoundError
except NameError:
    FileNotFoundError = IOError


def test_invalid_rcfile():
    from jobmon.server.the_server_config import get_the_server_config
    from jobmon.server.config import InvalidConfig
    with pytest.raises(InvalidConfig):
        get_the_server_config().from_file(__file__)


def test_server_config_command_line():
    from jobmon.server.the_server_config import get_the_server_config
    conn_str = get_the_server_config().conn_str
    opts_dct = {"conn_str": "foo"}
    get_the_server_config().apply_opts_dct(opts_dct)
    assert get_the_server_config().conn_str == 'foo'

    # reset
    os.environ['CONN_STR'] = conn_str
