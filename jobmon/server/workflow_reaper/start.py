import logging
import sys
from typing import Optional


from jobmon.server.workflow_reaper.reaper_config import WorkflowReaperConfig
from jobmon.server.workflow_reaper.notifiers import SlackNotifier
from jobmon.server.workflow_reaper.workflow_reaper import WorkflowReaper
from jobmon.requester import Requester


def start_workflow_reaper(workflow_reaper_config: Optional[WorkflowReaperConfig] = None
                          ) -> None:
    """Start monitoring for lost workflow runs"""

    logging.basicConfig(stream=sys.stdout, level=logging.INFO)

    if workflow_reaper_config is None:
        workflow_reaper_config = WorkflowReaperConfig.from_defaults()

    if workflow_reaper_config.slack_token and workflow_reaper_config.slack_api_url:
        wf_notifier = SlackNotifier(
            slack_api_url=workflow_reaper_config.slack_api_url,
            token=workflow_reaper_config.slack_token,
            default_channel=workflow_reaper_config.slack_channel_default)
        wf_sink = wf_notifier.send
    else:
        wf_sink = None

    requester = Requester(workflow_reaper_config.url)
    reaper = WorkflowReaper(
        poll_interval_minutes=workflow_reaper_config.poll_interval_minutes,
        loss_threshold=workflow_reaper_config.loss_threshold,
        requester=requester,
        wf_notification_sink=wf_sink)
    reaper.monitor_forever()
