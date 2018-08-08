import pytest
import sys
from sqlalchemy.exc import OperationalError


if sys.version_info < (3, 0):
    from exceptions import SystemExit as Py2Py3Exit
else:
    Py2Py3Exit = ValueError


def test_invalid_sub_command():
    from jobmon.cli import parse_args
    with pytest.raises(Py2Py3Exit):
        # Should complain that jobmon requires a sub-command
        parse_args("--conn_str mysql://user:pass@host")
    with pytest.raises(Py2Py3Exit):
        # Should complain that jobmon requires a sub-command
        parse_args("")
    with pytest.raises(SystemExit):
        # Should complain that the sub-command is not recongnized
        parse_args("not_a_valid_subcommand")
    with pytest.raises(SystemExit):
        # Should complain that the option is not recognized
        parse_args("--not_a_real_option initdb")


def test_start_subcommand():
    # A service name should be required...
    from jobmon.cli import parse_args
    with pytest.raises(SystemExit):
        parse_args("start")

    # Must be health_monitor
    parse_args("start health_monitor")

    # ... and the start function and service name should be attached
    args = parse_args("start health_monitor")
    assert args.func.__name__ == "start"
    assert args.service == "health_monitor"

    # Not something else...
    with pytest.raises(SystemExit):
        parse_args("start foobar")


def test_initdb_subcommand():
    from jobmon.cli import apply_args_to_config, parse_args
    from .server_test_config import config
    conn_str = config.conn_str
    print(conn_str)

    args = parse_args("--conn_str mysql://not:a@real/database initdb")
    apply_args_to_config(args)

    # Since that conn_str should point to a certainly non-existent
    # database, the attempt to initialize a database there
    # should raise an operational error
    with pytest.raises(OperationalError):
        args.func(args)

    # reset
    config.apply_opts_dct({'conn_str': conn_str})
    from jobmon.server import database
    database.recreate_engine()
