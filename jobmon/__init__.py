from ._version import get_versions
from .setup_config import SetupCfg
if SetupCfg().is_test_mode():
    __version__ = SetupCfg().get_jobmon_version()
else:
    __version__ = get_versions()['version']
    del get_versions

RELEASE_NAME = "emu"


from jobmon.client.workflow.workflow import Workflow
from jobmon.client.workflow.bash_task import BashTask
from jobmon.client.workflow.r_task import RTask
from jobmon.client.workflow.python_task import PythonTask
from jobmon.client.workflow.stata_task import StataTask
