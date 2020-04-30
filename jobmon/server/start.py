
def start_workflow_reaper():
    """Start monitoring for lost workflow runs"""
    from jobmon.server import ServerConfig
    from jobmon.server.workflow_reaper.notifiers import SlackNotifier
    from jobmon.server.workflow_reaper.workflow_reaper import WorkflowReaper

    config = ServerConfig.from_defaults()
    if config.slack_token:
        wf_notifier = SlackNotifier(
            config.slack_token,
            config.wf_slack_channel)
        wf_sink = wf_notifier.send
    else:
        wf_sink = None

    reaper = WorkflowReaper(wf_notification_sink=wf_sink)
    reaper.monitor_forever()


def start_qpid_integration():
    """Start the qpid integration service"""
    import jobmon.server.integration.qpid.qpid_integrator as qpid
    qpid.maxpss_forever()


def start_uwsgi_based_web_service():
    import subprocess
    subprocess.run("/entrypoint.sh")
    subprocess.run("/start.sh")
