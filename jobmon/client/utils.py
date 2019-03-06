import itertools
import os
import signal
import subprocess

from paramiko.client import SSHClient, WarningPolicy

from cluster_utils.io import check_permissions, InvalidPermissions

from jobmon.exceptions import UnsafeSSHDirectory


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
    if perm_dict is None:
        perm_dict = _get_ssh_permission_dict()

    errors = ""
    for directory in perm_dict.keys():
        try:
            check_permissions(directory, perm_dict[directory])
        except InvalidPermissions as e:
            errors += e.args[0]  # first arg is exception message

    if errors:
        raise UnsafeSSHDirectory(errors)


def kill_remote_process(hostname, pid, signal_number=signal.SIGKILL):
    kill_cmd = 'kill -{sn} {pid}'.format(sn=signal_number, pid=pid)
    return _run_remote_command(hostname, kill_cmd)


def kill_remote_process_group(hostname, pgid, signal_number=signal.SIGKILL):
    kill_cmd = 'kill -{sn} -{pgid}'.format(sn=signal_number, pgid=pgid)
    return _run_remote_command(hostname, kill_cmd)


def _get_ssh_permission_dict():
    ssh_safety_lookup = {}

    # get the allowed values for home folder. '7' for the user '0, 1, 4, 5' for
    # others. home folder can't be writable for ssh to be secure
    non_user_home_perms = itertools.product("0145", repeat=2)
    home_perms = ["7" + "".join(x) for x in non_user_home_perms]
    ssh_safety_lookup[_home_dir] = home_perms

    # check the ssh dir itself. if it doesn't exist do nothing because
    # ssh-keygen will run later
    if os.path.exists(_ssh_dir):
        ssh_safety_lookup[_ssh_dir] = "700"

        # if ssh dir exists, check that the private key exists.
        # if private key exists, confirm permissions for private key and
        # authorized_keys file
        if os.path.exists(_ssh_keyfile):
            ssh_safety_lookup[_ssh_keyfile] = "600"
            for auth_key in _authorized_keyfiles:
                ssh_safety_lookup[auth_key] = ["644", "600"]  # 600 for legacy
    return ssh_safety_lookup


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
        chmod_cmd = "chmod 644 {}".format(akf)  # use 644 like pub key file
        subprocess.call(chmod_cmd, shell=True)
