import sys

if sys.version_info >= (3, 8):
    from importlib import metadata
else:
    import importlib_metadata as metadata

__version__ = metadata.version('jobmon_installer_ihme')


def install_config():
    """read in a server config json file"""
    import json
    import os
    from subprocess import check_output

    file = os.path.join(os.path.dirname(__file__), "server_config.json")
    with open(file) as f:
        config_params = json.load(f)
    config_command = "jobmon_config update --web_service_fqdn {host} --web_service_port {port}"
    check_output(config_command.format(**config_params).split())
