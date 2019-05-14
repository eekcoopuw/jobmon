import json
import subprocess


def conda_env(conda_info):
    return str(conda_info['default_prefix'].split("/")[-1])


def path_to_conda_bin(conda_info):
    return '{}/bin'.format(conda_info['root_prefix'])


def read_conda_info():
    conda_info = json.loads(
        subprocess.check_output(['conda', 'info', '--json']).decode())
    return conda_info
