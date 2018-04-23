import os
import signal
import subprocess

from paramiko.client import AutoAddPolicy, SSHClient, WarningPolicy


SSH_KEYFILE_NAME="jobmonauto_id_rsa"
_ssh_dir = os.path.realpath(os.path.expanduser("~/.ssh"))
_ssh_keyfile = os.path.join(_ssh_dir, SSH_KEYFILE_NAME)

_authorized_keyfiles = [os.path.join(_ssh_dir, fn) for fn in
                        ['authorized_keys', 'authorized_keys2']]


def kill_remote_process(hostname, pid, signal_number=signal.SIGKILL):
    keyfile = _setup_keyfile()
    client = SSHClient()
    client.set_missing_host_key_policy(WarningPolicy)
    client.connect(hostname, look_for_keys=False, key_filename=keyfile)
    kill_cmd = 'kill -{sn} {pid}'.format(sn=signal_number, pid=pid)
    _, stdout, stderr = client.exec_command(kill_cmd)
    exit_code = stdout.channel.recv_exit_status()
    stdout_str = stdout.read()
    stderr_str = stderr.read()
    client.close()
    return (exit_code, stdout_str, stderr_str)


def kill_remote_process_group(hostname, pgid, signal_number=signal.SIGKILL):
    keyfile = _setup_keyfile()
    client = SSHClient()
    client.set_missing_host_key_policy(WarningPolicy)
    client.connect(hostname, look_for_keys=False, key_filename=keyfile)
    kill_cmd = 'kill -{sn} -{pgid}'.format(sn=signal_number, pgid=pgid)
    _, stdout, stderr = client.exec_command(kill_cmd)
    exit_code = stdout.channel.recv_exit_status()
    stdout_str = stdout.read()
    stderr_str = stderr.read()
    client.close()
    return (exit_code, stdout_str, stderr_str)


def _setup_keyfile():
    if not _keyfile_exists():
        _create_keyfile()
        _add_keyfile_to_authorized_keys()
        _set_authorized_keys_perms()
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


def _set_authorized_keys_perms():
    for akf in _authorized_keyfiles:
        chmod_cmd = "chmod 600 {}".format(akf)
        subprocess.call(chmod_cmd, shell=True)
