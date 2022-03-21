from argparse import Namespace
import shlex
from typing import List, Optional


import argparse


class CLI:
    """Base CLI."""

    def __init__(self) -> None:
        """Initialize the CLI."""
        self.parser = argparse.ArgumentParser(description="Jobmon Installer CLI")
        self._subparsers = self.parser.add_subparsers(
            dest="sub_command", parser_class=argparse.ArgumentParser
        )
        update_config_parser = self._subparsers.add_parser("configure")
        update_config_parser.set_defaults(func=self.update_config)

    def main(self, argstr: Optional[str] = None) -> None:
        """Parse args."""
        args = self.parse_args(argstr)
        args.func(args)

    def parse_args(self, argstr: Optional[str] = None) -> Namespace:
        """Construct a parser, parse either sys.argv (default) or the provided argstr.

        Returns a Namespace. The Namespace should have a 'func' attribute which can be used to
        dispatch to the appropriate downstream function.
        """
        arglist: Optional[List[str]] = None
        if argstr is not None:
            arglist = shlex.split(argstr)
        args = self.parser.parse_args(arglist)
        return args

    @staticmethod
    def update_config(args: Namespace) -> None:
        """Update .jobmon.ini.

        Args:
            args: only --web_service_fqdn --web_service_port are expected.
        """
        from jobmon_installer_ihme import load_config
        from jobmon.cli import update_config
        update_config(**load_config(lower_case=True))


def main(argstr: Optional[str] = None) -> None:
    """Create CLI."""
    cli = CLI()
    cli.main(argstr)
