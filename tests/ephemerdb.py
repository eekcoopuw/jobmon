import getpass
import logging
import os
import shutil
import signal
import socket
import uuid
from subprocess import Popen, STDOUT
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

    def start(self):
        self.db_port = self._get_random_port()
        os.makedirs(self.datadir)
        os.makedirs(self.sockdir)
        cmd = (
            "MYSQL_DATABASE={db_name} "
            "MYSQL_RANDOM_ROOT_PASSWORD=yes "
            "MYSQL_USER={db_user} "
            "MYSQL_PASSWORD={db_pass} "
            "{singularity} exec "
            "-B {datadir}:/var/lib/mysql "
            "-B {sockdir}:/run/mysqld "
            "{mysql_image} "
            "/usr/local/bin/docker-entrypoint.sh mysqld --port={port}".
            format(db_name=self.db_name,
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
            shutil.rmtree(self.dbdir)
            self.db_proc = None
            self.db_port = None

    def _conn_str(self):
        if not self.db_port or not self.db_proc:
            raise NameError("DB not started")
        return "mysql://{user}:{dbpass}@{host}:{port}/{db}".format(
            user=self.db_user, dbpass=self.db_pass, host=socket.gethostname(),
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
