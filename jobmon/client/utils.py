import itertools
import os
import subprocess
from typing import Tuple

from paramiko.client import SSHClient, WarningPolicy

from cluster_utils.io import check_permissions, InvalidPermissions
from jobmon.exceptions import UnsafeSSHDirectory
from jobmon.client.client_logging import ClientLogging as logging


logger = logging.getLogger(__name__)

SSH_KEYFILE_NAME = "jobmonauto_id_rsa"
_home_dir = os.path.realpath(os.path.expanduser("~"))
_ssh_dir = os.path.join(_home_dir, ".ssh")
_ssh_keyfile = os.path.join(_ssh_dir, SSH_KEYFILE_NAME)

_authorized_keyfiles = [os.path.join(_ssh_dir, fn) for fn in
                        ['authorized_keys', 'authorized_keys2']]


def confirm_correct_perms(perm_dict=None):
    """Verify that the ssh directory of a user is secure.

    Raises:
        UnsafeSSHDirectory
    """
    logger.info("confirm_correct_perms")
    if perm_dict is None:
        perm_dict = _get_ssh_permission_dict()

    errors = ""
    for directory in perm_dict.keys():
        permission_violation = True
        for p in perm_dict[directory]:
            try:
                check_permissions(directory, p)
                permission_violation = False
            except InvalidPermissions:
                # ate exception on purpose;
                # raise exception after checking all possible permits
                pass
        if permission_violation:
            errors += "{d} permission not in {p} ".format(
                d=directory, p=perm_dict[directory])

    if errors:
        logger.error(str(errors))
        raise UnsafeSSHDirectory(errors)


def gently_kill_command(id: int) -> str:
    """
    Best to use SIGTERM because it gives well-behaved processes a chance
    to clean up their child processes before the hard kill. This is less
    likely to leave orphans:
    kill SIGTERM; sleep 5; kill SIGKILL
    """
    logger.debug("gently_kill_command id:{}".format(id))
    return f'kill -SIGTERM {id}; sleep 10; kill -SIGKILL {id}'


def kill_remote_process(hostname: str, pid: int) -> Tuple[int, str, str]:
    """
    Used during RESUME, not during RETRY.

    :param hostname: the host on which to kill
    :param pid: Process id
    :return: exit_code, stdout_str, stderr_str
    """
    logger.info("kill_remote_process hostname: {h} pid: {p}".format(h=hostname, p=pid))
    return _run_remote_command(hostname, gently_kill_command(pid))


def kill_remote_process_group(hostname: str, pgid: int) ->\
        Tuple[int, str, str]:
    """
    Now only used during RETRY on prod
    :param hostname: the host on which to kill
    :param pgid: process group id

    :return: exit_code, stdout_str, stderr_str
    """
    logger.info("kill_remote_process_group")
    logger.info("hostname: {h} pgid: {p}".format(h=hostname, p=pgid))
    return _run_remote_command(hostname, gently_kill_command(pgid))


def _get_ssh_permission_dict():
    logger.info("_get_ssh_permission_dict")
    ssh_safety_lookup = {}

    # get the allowed values for home folder. '7' for the user '0, 1, 4, 5' for
    # others. home folder can't be writable for ssh to be secure
    non_user_home_perms = itertools.product("0145", repeat=2)
    home_perms = ["7" + "".join(x) for x in non_user_home_perms]
    ssh_safety_lookup[_home_dir] = home_perms

    # check the ssh dir itself. if it doesn't exist do nothing because
    # ssh-keygen will run later
    if os.path.exists(_ssh_dir):
        ssh_safety_lookup[_ssh_dir] = ["700"]

        # if ssh dir exists, check that the private key exists.
        # if private key exists, confirm permissions for private key and
        # authorized_keys file
        if os.path.exists(_ssh_keyfile):
            ssh_safety_lookup[_ssh_keyfile] = ["600"]
            for auth_key in _authorized_keyfiles:
                ssh_safety_lookup[auth_key] = ["644", "600"]  # 600 for legacy
    return ssh_safety_lookup


def _run_remote_command(hostname: str, command: str) -> Tuple[int, str, str]:
    """
    Runs the command on that host, using paramiko/ssh.
    Uses byte conversion on stdout stderr so that it return strings

    :returns exit_code, stdout_str, stderr_str
    """
    logger.info(" _run_remote_command")
    keyfile = _setup_keyfile()
    client = SSHClient()
    client.set_missing_host_key_policy(WarningPolicy)
    client.connect(hostname, look_for_keys=False, key_filename=keyfile)
    _, stdout, stderr = client.exec_command(command)
    exit_code = stdout.channel.recv_exit_status()
    stdout_str = stdout.read().decode("utf-8")
    stderr_str = stderr.read().decode("utf-8")
    client.close()
    return exit_code, stdout_str, stderr_str


def _setup_keyfile():
    logger.info("_setup_keyfile")
    if not _keyfile_exists():
        logger.debug(
            "{} not found. Create it for the user.".format(_ssh_keyfile))
        _create_keyfile()
        _add_keyfile_to_authorized_keys()
        _set_authorized_keys_perms()
    else:
        for akf in _authorized_keyfiles:
            if _key_in_auth_keyfile(_ssh_keyfile, akf):
                logger.debug("Found {key} in {auth}".format(
                    key=_ssh_keyfile, auth=akf))
            else:
                logger.debug("Add {key} to {auth}".format(
                    key=_ssh_keyfile, auth=akf))
                append_cmd = 'cat {keyfile}.pub >> {akf}'.format(
                    keyfile=_ssh_keyfile, akf=akf)
                subprocess.call(append_cmd, shell=True)
    return "{}".format(_ssh_keyfile)


def _add_keyfile_to_authorized_keys(kfile=_ssh_keyfile,
                                    authfiles=_authorized_keyfiles):
    for akf in authfiles:
        append_cmd = 'cat {keyfile}.pub >> {akf}'.format(keyfile=kfile,
                                                         akf=akf)
        subprocess.call(append_cmd, shell=True)


def _keyfile_exists():
    return os.path.isfile(_ssh_keyfile)


def _create_keyfile():
    keygen_command = 'ssh-keygen -t rsa -f {} -q -N ""'.format(_ssh_keyfile)
    return subprocess.call(keygen_command, shell=True)


def _key_in_auth_keyfile(keyfile=_ssh_keyfile, authfile=_authorized_keyfiles[0]
                         ):
    k_file = open(keyfile, "r").read()
    a_file = open(authfile, "r").read()
    return k_file in a_file


def _set_authorized_keys_perms(files=_authorized_keyfiles):
    for akf in files:
        chmod_cmd = "chmod 644 {}".format(akf)  # use 644 like pub key file
        subprocess.call(chmod_cmd, shell=True)
