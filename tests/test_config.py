import pytest

from jobmon.config import GlobalConfig, InvalidConfig


try:
    FileNotFoundError
except NameError:
    FileNotFoundError = IOError


def test_no_rcfile():
    with pytest.raises(FileNotFoundError):
        GlobalConfig.from_file("thisisnotafile_foobarbaz_12345")


def test_invalid_rcfile():
    with pytest.raises(InvalidConfig):
        GlobalConfig.from_file(__file__)


def test_command_line(rcfile):
    opts_dct = {"conn_str": "foo",
                "host": "bar",
                "jsm_port": "1",
                "jqs_port": "3"}
    import pdb; pdb.set_trace()
    gc = GlobalConfig.from_file(rcfile)
    gc.apply_opts_dct(opts_dct)
    assert gc.conn_str == 'foo'
    assert gc.jsm_conn.host == 'bar'
    assert gc.jqs_conn.host == 'bar'
    assert gc.jsm_conn.port == '1'
    assert gc.jqs_conn.port == '3'
