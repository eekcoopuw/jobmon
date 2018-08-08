import pytest

from jobmon.server import config as server_config
from jobmon.client import config as client_config


try:
    FileNotFoundError
except NameError:
    FileNotFoundError = IOError


def test_no_rcfile():
    with pytest.raises(FileNotFoundError):
        server_config.GlobalConfig.from_file("thisisnotafile_foobarbaz_12345")
        client_config.GlobalConfig.from_file("thisisnotafile_foobarbaz_12345")


def test_invalid_rcfile():
    with pytest.raises(server_config.InvalidConfig):
        server_config.GlobalConfig.from_file(__file__)


def test_server_config_command_line():
    opts_dct = {"conn_str": "foo"}
    client_config.config.apply_opts_dct(opts_dct)
    assert client_config.config.conn_str == 'foo'


def test_client_config_command_line():
    opts_dct = {"host": "bar",
                "jsm_port": "1",
                "jqs_port": "3"}
    client_config.config.apply_opts_dct(opts_dct)
    assert client_config.config.conn_str == 'foo'
