
from .setup_config import SetupCfg
Conf = SetupCfg()
__version__ = Conf.get_jobmon_version()

RELEASE_NAME = "emu"

from jobmon.client.swarm.workflow.workflow import Workflow
from jobmon.client.swarm.workflow.bash_task import BashTask
from jobmon.client.swarm.workflow.r_task import RTask
from jobmon.client.swarm.workflow.python_task import PythonTask
from jobmon.client.swarm.workflow.stata_task import StataTask
