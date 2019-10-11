from jobmon.setup_config import SetupCfg
from ._version import get_versions
__version__ = get_versions()['version']
del get_versions

config = SetupCfg()

RELEASE_NAME = "fruitbat"
