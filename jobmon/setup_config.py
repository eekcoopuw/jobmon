import configparser
import os


class SetupCfg:

    __singleton = None

    def __new__(cls, *args, **kwargs):
        # singleton implementation
        if cls.__singleton is None:
            cls.__singleton = super().__new__(cls)
        return cls.__singleton

    def __init__(self, test_mode=None, existing_db=None, same_host=None,
                 db_only=None, slack_token=None, slack_api_url=None,
                 wf_slack_channel=None, node_slack_channel=None,
                 jobmon_server_hostname=None, jobmon_server_sqdn=None,
                 jobmon_service_port=None, jobmon_monitor_port=None,
                 jobmon_integration_service_port=None, jobmon_version=None,
                 reconciliation_interval=None, heartbeat_interval=None,
                 report_by_buffer=None, tag_prefix=None, internal_db_host=None,
                 internal_db_port=None, external_db_host=None,
                 external_db_port=None, jobmon_service_user_pwd=None,
                 existing_network=None, use_rsyslog=None, rsyslog_host=None,
                 rsyslog_port=None, rsyslog_protocol=None, qpid_uri=None,
                 qpid_cluster=None, qpid_pulling_interval=None,
                 max_update_per_second=None, loss_threshold=None, poll_interval=None):

        # get env for fallback
        self._env = os.environ

        # get config for secondary fallback
        config_file = os.path.join(os.path.dirname(__file__), 'jobmon.cfg')
        self._config = configparser.ConfigParser()
        self._config.read(config_file)

        # initialize vals
        self.test_mode = test_mode
        self.existing_db = existing_db
        self.same_host = same_host
        self.db_only = db_only
        self.slack_token = slack_token
        self.slack_api_url = slack_api_url
        self.wf_slack_channel = wf_slack_channel
        self.node_slack_channel = node_slack_channel
        self.jobmon_server_hostname = jobmon_server_hostname
        self.jobmon_server_sqdn = jobmon_server_sqdn
        self.jobmon_service_port = jobmon_service_port
        self.jobmon_monitor_port = jobmon_monitor_port
        self.jobmon_integration_service_port = jobmon_integration_service_port
        self.jobmon_version = jobmon_version
        self.reconciliation_interval = reconciliation_interval
        self.heartbeat_interval = heartbeat_interval
        self.report_by_buffer = report_by_buffer
        self.tag_prefix = tag_prefix
        self.internal_db_host = internal_db_host
        self.internal_db_port = internal_db_port
        self.external_db_host = external_db_host
        self.external_db_port = external_db_port
        self.jobmon_service_user_pwd = jobmon_service_user_pwd
        self.existing_network = existing_network
        self.use_rsyslog = use_rsyslog
        self.rsyslog_host = rsyslog_host
        self.rsyslog_port = rsyslog_port
        self.rsyslog_protocol = rsyslog_protocol
        self.qpid_uri = qpid_uri
        self.qpid_cluster = qpid_cluster
        self.qpid_pulling_interval = qpid_pulling_interval
        self.max_update_per_second = max_update_per_second
        self.loss_threshold = loss_threshold
        self.poll_interval = poll_interval

    @property
    def test_mode(self) -> bool:
        return self._is_test_mode

    @test_mode.setter
    def test_mode(self, val):
        if val is None:
            val = self._env.get("TEST_MODE")
        if val is None:
            val = self._config["basic values"]["test_mode"] == "True"
        else:
            val = val == "True"
        self._is_test_mode = val

    @property
    def existing_db(self) -> bool:
        return self._existing_db

    @existing_db.setter
    def existing_db(self, val):
        if val is None:
            val = self._env.get("EXISTING_DB")
        if val is None:
            val = self._config["basic values"]["existing_db"] == "True"
        else:
            val = val == "True"
        self._existing_db = val

    @property
    def same_host(self) -> bool:
        return self._same_host

    @same_host.setter
    def same_host(self, val):
        if val is None:
            val = self._env.get("SAME_HOST")
        if val is None:
            val = self._config["basic values"]["same_host"] == "True"
        else:
            val = val == "True"
        self._same_host = val

    @property
    def db_only(self) -> bool:
        return self._db_only

    @db_only.setter
    def db_only(self, val):
        if val is None:
            val = self._env.get("DB_ONLY")
        if val is None:
            val = self._config["basic values"]["db_only"] == "True"
        else:
            val = val == "True"
        self._db_only = val

    @property
    def slack_token(self) -> str:
        return self._slack_token

    @slack_token.setter
    def slack_token(self, val):
        if val is None:
            val = self._env.get("SLACK_TOKEN")
        if val is None:
            val = self._config["basic values"]["slack_token"]
        self._slack_token = val

    @property
    def slack_api_url(self) -> str:
        return self._slack_api_url

    @slack_api_url.setter
    def slack_api_url(self, val):
        if val is None:
            val = self._env.get("SLACK_API_URL")
        if val is None:
            val = self._config["basic values"]["slack_api_url"]
        self._slack_api_url = val

    @property
    def wf_slack_channel(self) -> str:
        return self._wf_slack_channel

    @wf_slack_channel.setter
    def wf_slack_channel(self, val):
        if val is None:
            val = self._env.get("WF_SLACK_CHANNEL")
        if val is None:
            val = self._config["basic values"]["wf_slack_channel"]
        self._wf_slack_channel = val

    @property
    def node_slack_channel(self) -> str:
        return self._node_slack_channel

    @node_slack_channel.setter
    def node_slack_channel(self, val):
        if val is None:
            val = self._env.get("NODE_SLACK_CHANNEL")
        if val is None:
            val = self._config["basic values"]["node_slack_channel"]
        self._node_slack_channel = val

    @property
    def jobmon_server_hostname(self) -> str:
        return self._jobmon_server_hostname

    @jobmon_server_hostname.setter
    def jobmon_server_hostname(self, val):
        if val is None:
            val = self._env.get("JOBMON_SERVER_HOSTNAME")
        if val is None:
            val = self._config["basic values"]["jobmon_server_hostname"]
        self._jobmon_server_hostname = val

    @property
    def jobmon_server_sqdn(self) -> str:
        return self._jobmon_server_sqdn

    @jobmon_server_sqdn.setter
    def jobmon_server_sqdn(self, val):
        if val is None:
            val = self._env.get("JOBMON_SERVER_SQDN")
        if val is None:
            val = self._config["basic values"]["jobmon_server_sqdn"]
        self._jobmon_server_sqdn = val

    @property
    def jobmon_service_port(self) -> int:
        return self._jobmon_service_port

    @jobmon_service_port.setter
    def jobmon_service_port(self, val):
        if val is None:
            val = self._env.get("JOBMON_SERVICE_PORT")
        if val is None:
            val = self._config["basic values"]["jobmon_service_port"]
        self._jobmon_service_port = int(val)

    @property
    def jobmon_monitor_port(self) -> int:
        return self._jobmon_monitor_port

    @jobmon_monitor_port.setter
    def jobmon_monitor_port(self, val):
        if val is None:
            val = self._env.get("JOBMON_MONITOR_PORT")
        if val is None:
            val = self._config["basic values"]["jobmon_monitor_port"]
        self._jobmon_monitor_port = int(val)

    @property
    def jobmon_version(self) -> str:
        return self._jobmon_version

    @jobmon_version.setter
    def jobmon_version(self, val):
        if self.test_mode:
            if val is None:
                val = self._env.get("JOBMON_VERSION")
            if val is None:
                val = self._config["basic values"]["jobmon_version"]
        else:
            if val is not None:
                raise ValueError("cannot set jobmon version unless test_mode"
                                 " is set to True")
            else:
                from jobmon import __version__
                val = __version__
        if val is not None:
            import jobmon
            setattr(jobmon, "__version__", val)
        self._jobmon_version = val

    @property
    def reconciliation_interval(self) -> int:
        return self._reconciliation_interval

    @reconciliation_interval.setter
    def reconciliation_interval(self, val):
        if val is None:
            val = self._env.get("RECONCILIATION_INTERVAL")
        if val is None:
            val = self._config["basic values"]["reconciliation_interval"]
        self._reconciliation_interval = int(val)

    @property
    def heartbeat_interval(self) -> int:
        return self._heartbeat_interval

    @heartbeat_interval.setter
    def heartbeat_interval(self, val):
        if val is None:
            val = self._env.get("HEARTBEAT_INTERVAL")
        if val is None:
            val = self._config["basic values"]["heartbeat_interval"]
        self._heartbeat_interval = int(val)

    @property
    def report_by_buffer(self) -> float:
        return self._report_by_buffer

    @report_by_buffer.setter
    def report_by_buffer(self, val):
        if val is None:
            val = self._env.get("REPORT_BY_BUFFER")
        if val is None:
            val = self._config["basic values"]["report_by_buffer"]
        self._report_by_buffer = float(val)

    @property
    def tag_prefix(self) -> str:
        return self._tag_prefix

    @tag_prefix.setter
    def tag_prefix(self, val):
        if val is None:
            val = self._env.get("TAG_PREFIX")
        if val is None:
            val = self._config["basic values"]["tag_prefix"]
        self._tag_prefix = val

    @property
    def internal_db_host(self) -> str:
        return self._internal_db_host

    @internal_db_host.setter
    def internal_db_host(self, val):
        if val is None:
            val = self._env.get("INTERNAL_DB_HOST")
        if val is None:
            val = self._config["db"]["internal_db_host"]
        self._internal_db_host = val

    @property
    def internal_db_port(self) -> str:
        return self._internal_db_port

    @internal_db_port.setter
    def internal_db_port(self, val):
        if val is None:
            val = self._env.get("INTERNAL_DB_PORT")
        if val is None:
            val = self._config["db"]["internal_db_port"]
        self._internal_db_port = val

    @property
    def external_db_host(self) -> str:
        return self._external_db_host

    @external_db_host.setter
    def external_db_host(self, val):
        if val is None:
            val = self._env.get("EXTERNAL_DB_HOST")
        if val is None:
            val = self._config["db"]["external_db_host"]
        self._external_db_host = val

    @property
    def external_db_port(self) -> str:
        return self._external_db_port

    @external_db_port.setter
    def external_db_port(self, val):
        if val is None:
            val = self._env.get("EXTERNAL_DB_PORT")
        if val is None:
            val = self._config["db"]["external_db_port"]
        self._external_db_port = val

    @property
    def jobmon_service_user_pwd(self) -> str:
        return self._jobmon_service_user_pwd

    @jobmon_service_user_pwd.setter
    def jobmon_service_user_pwd(self, val):
        if val is None:
            val = self._env.get("JOBMON_SERVICE_USER_PWD")
        if val is None:
            val = self._config["existing db"]["jobmon_service_user_pwd"]
        self._jobmon_service_user_pwd = val

    @property
    def existing_network(self) -> str:
        return self._existing_network

    @existing_network.setter
    def existing_network(self, val):
        if val is None:
            val = self._env.get("EXISTING_NETWORK")
        if val is None:
            val = self._config["same host"]["existing_network"]
        self._existing_network = val

    @property
    def use_rsyslog(self) -> bool:
        return self._use_rsyslog

    @use_rsyslog.setter
    def use_rsyslog(self, val):
        if val is None:
            val = self._env.get("USE_RSYSLOG")
        if val is None:
            val = self._config["basic values"]["use_rsyslog"] == "True"
        self._use_rsyslog = val

    @property
    def rsyslog_host(self) -> str:
        return self._rsyslog_host

    @rsyslog_host.setter
    def rsyslog_host(self, val):
        if val is None:
            val = self._env.get("RSYSLOG_HOST")
        if val is None:
            val = self._config["rsyslog"]["host"]
        self._rsyslog_host = val

    @property
    def rsyslog_port(self) -> int:
        return self._rsyslog_port

    @rsyslog_port.setter
    def rsyslog_port(self, val):
        if val is None:
            val = self._env.get("RSYSLOG_PORT")
        if val is None:
            val = self._config["rsyslog"]["port"]
        self._rsyslog_port = int(val)

    @property
    def rsyslog_protocol(self) -> int:
        return self._rsyslog_protocol

    @rsyslog_protocol.setter
    def rsyslog_protocol(self, val):
        if val is None:
            val = self._env.get("RSYSLOG_PROTOCOL")
        if val is None:
            val = self._config["rsyslog"]["protocol"]
        self._rsyslog_protocol = val

    @property
    def qpid_uri(self) -> str:
        return self._qpid_uri

    @qpid_uri.setter
    def qpid_uri(self, val):
        if val is None:
            val = self._env.get("QPID_URI")
        if val is None:
            val = self._config["qpid"]["uri"]
        self._qpid_uri = val

    @property
    def qpid_cluster(self) -> str:
        return self._qpid_cluster

    @qpid_cluster.setter
    def qpid_cluster(self, val):
        if val is None:
            val = self._env.get("QPID_CLUSTER")
        if val is None:
            val = self._config["qpid"]["cluster"]
        self._qpid_cluster = val

    @property
    def jobmon_integration_service_port(self) -> str:
        return self._jobmon_integration_service_port

    @jobmon_integration_service_port.setter
    def jobmon_integration_service_port(self, val):
        if val is None:
            val = self._env.get("JOBMON_INTEGRATION_SERVICE_PORT")
        if val is None:
            val = self._config["basic values"]["jobmon_integration_service_port"]
        self._jobmon_integration_service_port = val

    @property
    def qpid_pulling_interval(self) -> str:
        return self._qpid_pulling_interval

    @qpid_pulling_interval.setter
    def qpid_pulling_interval(self, val):
        if val is None:
            val = self._env.get("QPID_PULLING_INTERVAL")
        if val is None:
            val = self._config["qpid"]["pulling_interval"]
        self._qpid_pulling_interval = int(val)

    @property
    def max_update_per_second(self) -> str:
        return self._max_update_per_second

    @max_update_per_second.setter
    def max_update_per_second(self, val):
        if val is None:
            val = self._env.get("MAX_UPDATE_PER_SECOND")
        if val is None:
            val = self._config["qpid"]["max_update_per_second"]
        self._max_update_per_second = int(val)

    @property
    def loss_threshold(self) -> int:
        return self._loss_threshold

    @loss_threshold.setter
    def loss_threshold(self, val):
        if val is None:
            val = self._env.get("LOSS_THRESHOLD")
        if val is None:
            val = self._config["basic values"]["loss_threshold"]
        self._loss_threshold = int(val)

    @property
    def poll_interval(self) -> int:
        return self._poll_interval

    @poll_interval.setter
    def poll_interval(self, val):
        if val is None:
            val = self._env.get("POLL_INTERVAL")
        if val is None:
            val = self._config["basic values"]["poll_interval"]
        self._poll_interval = int(val)
