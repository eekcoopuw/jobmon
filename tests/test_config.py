import pytest

from jobmon.config import GlobalConfig, InvalidConfig


def test_no_rcfile():
    with pytest.raises(FileNotFoundError):
        GlobalConfig.from_file("thisisnotafile_foobarbaz_12345")


def test_invalid_rcfile():
    with pytest.raises(InvalidConfig):
        GlobalConfig.from_file(__file__)


def test_command_line(rcfile):
    opts_dct = {"conn_str": "foo",
                "host": "bar",
                "jsm_rep_port": "1",
                "jsm_pub_port": "2",
                "jqs_port": "3"}
    gc = GlobalConfig.from_file(rcfile)
    gc.apply_opts_dct(opts_dct)
    assert gc.conn_str == 'foo'
    assert gc.jm_rep_conn.host == 'bar'
    assert gc.jm_pub_conn.host == 'bar'
    assert gc.jqs_rep_conn.host == 'bar'
    assert gc.jqs_rep_conn.port == '3'
