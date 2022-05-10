from __future__ import annotations

from typing import Any

from jobmon.config import CLI, ParserDefaults


class UsageConfig:
    """Integrator configuration."""

    @classmethod
    def from_defaults(cls: Any) -> UsageConfig:
        """Set up config from defaults if no alternates specified."""
        cli = CLI()
        ParserDefaults.db_host(cli.parser)
        ParserDefaults.db_port(cli.parser)
        ParserDefaults.db_user(cli.parser)
        ParserDefaults.db_pass(cli.parser)
        ParserDefaults.db_name(cli.parser)
        ParserDefaults.db_host_slurm_sdb(cli.parser)
        ParserDefaults.db_port_slurm_sdb(cli.parser)
        ParserDefaults.db_user_slurm_sdb(cli.parser)
        ParserDefaults.db_pass_slurm_sdb(cli.parser)
        ParserDefaults.db_name_slurm_sdb(cli.parser)
        # slurm new config
        ParserDefaults.slurm_polling_interval(cli.parser)
        ParserDefaults.slurm_max_update_per_second(cli.parser)
        ParserDefaults.slurm_cluster(cli.parser)
        ParserDefaults.integrator_never_retire(cli.parser)

        # passing an empty string forces this method to ignore sys.argv
        args = cli.parse_args("")

        return cls(
            db_host=args.db_host,
            db_port=args.db_port,
            db_user=args.db_user,
            db_pass=args.db_pass,
            db_name=args.db_name,
            db_host_slurm_sdb=args.db_host_slurm_sdb,
            db_port_slurm_sdb=args.db_port_slurm_sdb,
            db_user_slurm_sdb=args.db_user_slurm_sdb,
            db_pass_slurm_sdb=args.db_pass_slurm_sdb,
            db_name_slurm_sdb=args.db_name_slurm_sdb,
            # slurm
            slurm_polling_interval=args.slurm_polling_interval,
            slurm_max_update_per_second=args.slurm_max_update_per_second,
            slurm_cluster=args.slurm_cluster,
            # usage integrator
            integrator_never_retire=args.integrator_never_retire,
        )

    def __init__(
        self,
        db_host: str,
        db_port: str,
        db_user: str,
        db_pass: str,
        db_name: str,
        db_host_slurm_sdb: str,
        db_port_slurm_sdb: str,
        db_user_slurm_sdb: str,
        db_pass_slurm_sdb: str,
        db_name_slurm_sdb: str,
        # slurm
        slurm_polling_interval: int,
        slurm_max_update_per_second: int,
        slurm_cluster: str,
        # whether to retire failed to integrate ti
        integrator_never_retire: bool = True
    ) -> None:
        """Initialization of the integrator configuration."""
        self.db_host = db_host
        self.db_port = db_port
        self.db_user = db_user
        self.db_pass = db_pass
        self.db_name = db_name
        self.db_host_slurm_sdb = db_host_slurm_sdb
        self.db_port_slurm_sdb = db_port_slurm_sdb
        self.db_user_slurm_sdb = db_user_slurm_sdb
        self.db_pass_slurm_sdb = db_pass_slurm_sdb
        self.db_name_slurm_sdb = db_name_slurm_sdb
        # slurm
        self.slurm_polling_interval = slurm_polling_interval
        self.slurm_max_update_per_second = slurm_max_update_per_second
        self.slurm_cluster = slurm_cluster
        self.integrator_never_retire = integrator_never_retire

    @property
    def conn_str(self) -> str:
        """Connection string to connect to the jobmon database."""
        conn_str = "mysql://{user}:{pw}@{host}:{port}/{db}".format(
            user=self.db_user,
            pw=self.db_pass,
            host=self.db_host,
            port=self.db_port,
            db=self.db_name,
        )
        return conn_str

    @property
    def conn_slurm_sdb_str(self) -> str:
        """Connection string to connect to the slurm_sdb database."""
        conn_slurm_sdb_str = "mysql://{user}:{pw}@{host}:{port}/{db}".format(
            user=self.db_user_slurm_sdb,
            pw=self.db_pass_slurm_sdb,
            host=self.db_host_slurm_sdb,
            port=self.db_port_slurm_sdb,
            db=self.db_name_slurm_sdb,
        )
        return conn_slurm_sdb_str
