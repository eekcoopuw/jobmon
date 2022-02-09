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

    file = os.path.join(os.path.dirname(__file__), "server_config.json")
    with open(file) as f:
        config_params = json.load(f)

    # Add to the user's environment
    os.environ.update(**config_params)