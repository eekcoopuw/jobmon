from datetime import datetime, timedelta

from jobmon.serializers import SerializeWorkflowRun
from jobmon.server.server_logging import jobmonLogging as logging
from jobmon.client import shared_requester
from jobmon.server.server_config import ServerConfig
from jobmon.server import create_app
from jobmon.models import DB
from jobmon import __version__

logger = logging.getLogger(__file__)


class WorkflowReaper(object):
    def __init__(self, loss_threshold= 5, poll_interval=10,
                 wf_notification_sink=None,requester=shared_requester,
                 app=None):
        logger.debug(logging.myself())

        self._requester = requester
        self._loss_threshold = timedelta(minutes=loss_threshold)
        self._poll_interval = poll_interval
        self._wf_notification_sink = wf_notification_sink

        #TODO Handle scenario when poll_interval is less than loss_threshold

        # construct flask app
        if app is None:
            config = ServerConfig.from_defaults()
            logger.debug("DB config: {}".format(config))
            self.app = create_app(config)
            DB.init_app(self.app)
        else:
            self.app = app

        # get database name
        uri = self.app.config['SQLALCHEMY_DATABASE_URI']
        self._database = uri.split("/")[-1]

    def _monitor_forever(self):
        logger.debug(logging.myself())
        if self._wf_notification_sink is not None:
            self._wf_notification_sink(
                msg=f"health monitor v{__version__} is alive"
            )
        #TODO endless while loop in try catch

    def _get_active_workflow_runs(self):
        logger.debug(logging.myself())
        app_route = "/workflow_run/active"
        _, result = self._requester.send_request(
            app_route=app_route,
            message={},
            request_type='get')
        workflow_runs = [
            SerializeWorkflowRun.kwargs_from_wire(wire_tuple)["id"]
            for wire_tuple in result["heartbeat_date"]
        ]
        return workflow_runs

    def _get_lost_worfklow_runs(self):
        logger.debug(logging.myself())

    def _has_lost_workflow_run(self, workflow_run):
        logger.debug(logging.myself())

    def _register_lost_workflow_runs(self):
        logger.debug(logging.myself())
