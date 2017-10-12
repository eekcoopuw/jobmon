import atexit
import getpass
import logging
import os
import shutil
import signal
import socket
import uuid
from subprocess import check_output, Popen, STDOUT
from time import sleep

from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError


logger = logging.getLogger(__name__)


class DBUnavailable(Exception):
    pass


class EphemerDB(object):

    def __init__(self):
        self.dbdir = self._get_unique_dirname()
        self.datadir = "{}/data".format(self.dbdir)
        self.sockdir = "{}/sock".format(self.dbdir)
        self.singularity_bin = "/share/local/singularity/bin/singularity"
        self.mysql_image_file = "/ihme/singularity-images/mysql/mysql.img"

        self.db_port = None
        self.db_proc = None

        self.db_name = 'singularity'
        self.db_user = 'sing'
        self.db_pass = 'song'
        self.db_root_pass = 'singularity_root'

    def execute_sql_script(self, sql_script):
        cmd = ("mysql -h {host} -P {port} --user=root --password={pw} "
               "< {sql}".format(host=socket.gethostname(),
                                port=self.db_port,
                                pw=self.db_root_pass,
                                sql=sql_script))
        stdout = check_output(cmd, shell=True)
        return stdout

    def start(self):
        atexit.register(self.stop)
        self.db_port = self._get_random_port()
        os.makedirs(self.datadir)
        os.makedirs(self.sockdir)
        cmd = (
            "MYSQL_DATABASE={db_name} "
            "MYSQL_ROOT_PASSWORD={root_pw} "
            "MYSQL_USER={db_user} "
            "MYSQL_PASSWORD={db_pass} "
            "{singularity} exec "
            "-B {datadir}:/var/lib/mysql "
            "-B {sockdir}:/run/mysqld "
            "{mysql_image} "
            "/usr/local/bin/docker-entrypoint.sh mysqld --port={port}".
            format(db_name=self.db_name,
                   root_pw=self.db_root_pass,
                   db_user=self.db_user,
                   db_pass=self.db_pass,
                   singularity=self.singularity_bin,
                   datadir=self.datadir,
                   sockdir=self.sockdir,
                   mysql_image=self.mysql_image_file,
                   port=self.db_port))
        FNULL = open(os.devnull, 'w')
        self.db_proc = Popen(cmd, shell=True,
                             stdout=FNULL, stderr=STDOUT,
                             preexec_fn=os.setsid)
        conn_str = self._conn_str()

        eng = create_engine(conn_str)
        attempt = 0
        logger.info("Waiting for db to come online")
        while attempt < 5:
            attempt = attempt + 1
            try:
                conn = eng.connect()
                conn.close()
                connected = True
                logger.debug("Succeeded on attempt {}".format(attempt))
            except OperationalError:
                logger.debug("Failed on attempt {}".format(attempt))
                sleep_time = attempt*2
                sleep(sleep_time)
                connected = False
                continue
        eng.dispose()
        if not connected:
            raise DBUnavailable("Database could not be started at "
                                "{}".format(conn_str))
        logger.info("DB is online at {}".format(conn_str))
        return conn_str

    def stop(self):
        if self.db_proc:
            os.killpg(os.getpgid(self.db_proc.pid), signal.SIGTERM)
            self.db_proc = None
            self.db_port = None

        # Note that we're ignoring errors because the previous killpg
        # call might cause mysql.sock to disappear mid rmtree...
        shutil.rmtree(self.dbdir, ignore_errors=True)

    def _conn_str(self, as_root=False):
        if not self.db_port or not self.db_proc:
            raise NameError("DB not started")
        if as_root:
            user = 'root'
            dbpass = self.db_root_pass
        else:
            user = self.db_user
            dbpass = self.db_pass
        return "mysql://{user}:{dbpass}@{host}:{port}/{db}".format(
            user=user, dbpass=dbpass, host=socket.gethostname(),
            port=self.db_port, db=self.db_name)

    def _get_unique_dirname(self):
        return "/tmp/ephemerdb-{user}-{uuid}".format(user=getpass.getuser(),
                                                     uuid=uuid.uuid4())

    def _get_random_port(self):
        sock = socket.socket()
        sock.bind(('', 0))
        _, port = sock.getsockname()
        sock.close()
        return port
