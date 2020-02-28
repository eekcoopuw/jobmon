from ._version import get_versions
__version__ = get_versions()['version']
del get_versions

from jobmon.setup_config import SetupCfg

config = SetupCfg()
RELEASE_NAME = "guppy"
