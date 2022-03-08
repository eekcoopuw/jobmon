import json
import os
import sys
from typing import Dict

if sys.version_info >= (3, 8):
    from importlib import metadata
else:
    import importlib_metadata as metadata

__version__ = metadata.version('jobmon_installer_ihme')


def load_config(lower_case: bool = False) -> Dict[str, str]:
    file = os.path.join(os.path.dirname(__file__), "server_config.json")
    with open(file) as f:
        config_params = json.load(f)

    if lower_case:
        lowered_params = {k.lower(): v for k, v in config_params.items()}
        config_params = lowered_params
    return config_params


def install_config():
    """read in a server config json file"""

    # Add to the user's environment
    print(
        "Temporarily configuring jobmon with environment variables. To get rid of this message"
        " permanently run the following command in your shell: `jobmon_ihme configure`."
    )
    os.environ.update(**load_config(lower_case=False))
