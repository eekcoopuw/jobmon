import pytest
from sqlalchemy.exc import OperationalError

from jobmon.cli import apply_args_to_config, parse_args


def test_invalid_sub_command():
    with pytest.raises(ValueError):
        # Should complain that jobmon requires a sub-command
        parse_args("--conn_str mysql://user:pass@host")
    with pytest.raises(ValueError):
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
    with pytest.raises(SystemExit):
        parse_args("start")

    # Either job_query_server or job_state_manager
    parse_args("start job_query_server")

    # ... and the start function and service name should be attached
    args = parse_args("start job_state_manager")
    assert args.func.__name__ == "start"
    assert args.service == "job_state_manager"

    # Not something else...
    with pytest.raises(SystemExit):
        parse_args("start foobar")


def test_initdb_subcommand():
    args = parse_args("--conn_str mysql://not:a@real/database initdb")
    apply_args_to_config(args)

    # Since that conn_str should point to a certainly non-existent
    # database, the attempt to initialize a database there
    # should raise and operational error
    with pytest.raises(OperationalError):
        args.func(args)
