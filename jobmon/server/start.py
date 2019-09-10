
def start_health_monitor():
    """Start monitoring for lost workflow runs"""
    from jobmon.server import ServerConfig
    from jobmon.server.health_monitor.notifiers import SlackNotifier
    from jobmon.server.health_monitor.health_monitor import HealthMonitor

    config = ServerConfig.from_defaults()
    if config.slack_token:
        wf_notifier = SlackNotifier(
            config.slack_token,
            config.wf_slack_channel)
        wf_sink = wf_notifier.send
        node_notifier = SlackNotifier(
            config.slack_token,
            config.node_slack_channel)
        node_sink = node_notifier.send
    else:
        wf_sink = None
        node_sink = None
    hm = HealthMonitor(wf_notification_sink=wf_sink,
                       node_notification_sink=node_sink)
    hm.monitor_forever()


def start_uwsgi_based_web_service():
    import subprocess
    subprocess.run("/entrypoint.sh")
    subprocess.run("/start.sh")
