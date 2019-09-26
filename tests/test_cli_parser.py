import pytest

from jobmon.cli import CLI


def test_invalid_sub_command():
    cli = CLI()

    with pytest.raises(ValueError):
        # Should complain that jobmon requires a sub-command
        cli.parse_args("")
    with pytest.raises(SystemExit):
        # Should complain that the sub-command is not recongnized
        cli.parse_args("not_a_valid_subcommand")
    with pytest.raises(SystemExit):
        # Should complain that the option is not recognized
        cli.parse_args("--not_a_real_option initdb")


@pytest.mark.parametrize("valid_subcommand",
                         ["health_monitor", "web_service"])
def test_start_subcommand(valid_subcommand):
    cli = CLI()

    # A service name should be required...
    with pytest.raises(SystemExit):
        cli.parse_args("start")

    # Must be valid subcommand
    args = cli.parse_args("start {}".format(valid_subcommand))

    # ... and the start function and service name should be attached
    assert args.func.__name__ == "start"
    assert args.service == valid_subcommand

    # Not something else...
    with pytest.raises(SystemExit):
        cli.parse_args("start foobar")
