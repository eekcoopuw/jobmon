import os
import signal
import subprocess
import logging

from paramiko.client import SSHClient, WarningPolicy

logger = logging.getLogger(__name__)

SSH_KEYFILE_NAME = "jobmonauto_id_rsa"
_ssh_dir = os.path.realpath(os.path.expanduser("~/.ssh"))
_ssh_keyfile = os.path.join(_ssh_dir, SSH_KEYFILE_NAME)

_authorized_keyfiles = [os.path.join(_ssh_dir, fn) for fn in
                        ['authorized_keys', 'authorized_keys2']]


def kill_remote_process(hostname, pid, signal_number=signal.SIGKILL):
    kill_cmd = 'kill -{sn} {pid}'.format(sn=signal_number, pid=pid)
    return _run_remote_command(hostname, kill_cmd)


def kill_remote_process_group(hostname, pgid, signal_number=signal.SIGKILL):
    kill_cmd = 'kill -{sn} -{pgid}'.format(sn=signal_number, pgid=pgid)
    return _run_remote_command(hostname, kill_cmd)


def _run_remote_command(hostname, command):
    keyfile = _setup_keyfile()
    client = SSHClient()
    client.set_missing_host_key_policy(WarningPolicy)
    client.connect(hostname, look_for_keys=False, key_filename=keyfile)
    _, stdout, stderr = client.exec_command(command)
    exit_code = stdout.channel.recv_exit_status()
    stdout_str = stdout.read()
    stderr_str = stderr.read()
    client.close()
    return (exit_code, stdout_str, stderr_str)


def _setup_keyfile():
    if not _keyfile_exists():
        logger.debug("{} not found. Create it for the user.".format(_ssh_keyfile))
        _create_keyfile()
        _add_keyfile_to_authorized_keys()
        _set_authorized_keys_perms()
    else:
        for akf in _authorized_keyfiles:
            if _key_in_auth_keyfile(_ssh_keyfile, akf):
                logger.debug("Found {key} in {auth}".format(key=_ssh_keyfile, auth=akf))
            else:
                logger.debug("Add {key} to {auth}".format(key=_ssh_keyfile, auth=akf))
                append_cmd = 'cat {keyfile}.pub >> {akf}'.format(keyfile=_ssh_keyfile,
                                                                 akf=akf)
                subprocess.call(append_cmd, shell=True)
    return "{}".format(_ssh_keyfile)


def _add_keyfile_to_authorized_keys():
    for akf in _authorized_keyfiles:
        append_cmd = 'cat {keyfile}.pub >> {akf}'.format(keyfile=_ssh_keyfile,
                                                         akf=akf)
        subprocess.call(append_cmd, shell=True)


def _keyfile_exists():
    return os.path.isfile(_ssh_keyfile)


def _create_keyfile():
    keygen_command = 'ssh-keygen -t rsa -f {} -q -N ""'.format(_ssh_keyfile)
    return subprocess.call(keygen_command, shell=True)


def _key_in_auth_keyfile(keyfile=_ssh_keyfile, authfile=_authorized_keyfiles[0]):
    k_file = open(keyfile, "r").read()
    a_file = open(authfile, "r").read()
    return k_file in a_file


def _set_authorized_keys_perms():
    for akf in _authorized_keyfiles:
        chmod_cmd = "chmod 600 {}".format(akf)
        subprocess.call(chmod_cmd, shell=True)
