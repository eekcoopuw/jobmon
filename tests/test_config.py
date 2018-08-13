import pytest
import os


try:
    FileNotFoundError
except NameError:
    FileNotFoundError = IOError


def test_no_rcfile():
    from jobmon.server.the_server_config import get_the_server_config
    from jobmon.client.the_client_config import get_the_client_config
    with pytest.raises(FileNotFoundError):
        get_the_server_config().from_file("thisisnotafile_foobarbaz_12345")
        get_the_client_config().from_file("thisisnotafile_foobarbaz_12345")


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
    os.environ['conn_str'] = conn_str


def test_client_config_command_line():
    from jobmon.client.the_client_config import get_the_client_config
    orig_cfg = get_the_client_config()
    orig_opts = {"host": orig_cfg.host,
                 "jsm_port": "{}".format(orig_cfg.jsm_port),
                 "jqs_port": "{}".format(orig_cfg.jqs_port)}
    opts_dct = {"host": "bar",
                "jsm_port": "1",
                "jqs_port": "3"}
    get_the_client_config().apply_opts_dct(opts_dct)
    assert get_the_client_config().host == 'bar'

    # reset
    get_the_client_config().apply_opts_dct(orig_opts)
    os.environ['host'] = orig_cfg.host
