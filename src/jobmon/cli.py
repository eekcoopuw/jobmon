from typing import Optional

import configargparse

from jobmon.config import CLI, INSTALLED_CONFIG_FILE, PARSER_KWARGS, ParserDefaults


def update_config(host: str, port: int) -> None:
    """Update .jobmon.ini.

    Args:
        host: web server host
        port: web server port
    """
    import os
    import configparser
    import requests

    # validate the updated url is reachable
    url = f"http://{host}:{port}"
    try:
        _ = requests.get(url)
    except requests.ConnectionError:
        raise ValueError(f"URL {url} is not reachable.")

    if os.path.isfile(INSTALLED_CONFIG_FILE):
        edit = configparser.ConfigParser()
        edit.read(INSTALLED_CONFIG_FILE)
        client = edit["client"]
        if client["web_service_fqdn"] == host and client["web_service_port"] == port:
            print(
                "The new values are the same as in the config file. "
                "No update is made to the config file."
            )
            return
        else:
            client["web_service_fqdn"] = host
            client["web_service_port"] = str(port)
            with open(INSTALLED_CONFIG_FILE, "w") as configfile:
                edit.write(configfile)
    else:
        config = configparser.ConfigParser()
        config.add_section("client")
        config.set("client", "web_service_fqdn", host)
        config.set("client", "web_service_port", str(port))
        with open(INSTALLED_CONFIG_FILE, "w") as configfile:
            config.write(configfile)

    print(
        f"Config file {INSTALLED_CONFIG_FILE} has been updated",
        f"with new web_service_fqdn = {host} web_service_port = {port}.",
    )

    return


class GlobalCLI(CLI):
    """Client command line interface for workflow/task status and concurrency limiting."""

    def __init__(self) -> None:
        """Initialization of client CLI."""
        self.parser = configargparse.ArgumentParser(add_help=False, **PARSER_KWARGS)
        self._subparsers = self.parser.add_subparsers(
            dest="sub_command", parser_class=configargparse.ArgumentParser
        )
        self._add_update_config_subparser()

    @staticmethod
    def update_config(args: configargparse.Namespace) -> None:
        """Update .jobmon.ini.

        Args:
            args: only --web_service_fqdn --web_service_port are expected.
        """
        update_config(args.web_service_fqdn, args.web_service_port)

    def _add_update_config_subparser(self) -> None:
        parser_kwargs = dict(PARSER_KWARGS)
        parser_kwargs.pop("args_for_setting_config_path")
        update_config_parser = self._subparsers.add_parser("update", **parser_kwargs)
        update_config_parser.set_defaults(func=self.update_config)
        ParserDefaults.web_service_fqdn(update_config_parser)
        ParserDefaults.web_service_port(update_config_parser)


def main(argstr: Optional[str] = None) -> None:
    """Create CLI."""
    cli = GlobalCLI()
    cli.main(argstr)
