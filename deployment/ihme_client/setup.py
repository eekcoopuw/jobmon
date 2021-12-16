import json
import os
from setuptools import setup
from setuptools.command.develop import develop
from setuptools.command.install import install
from subprocess import check_output


def read_config():
    """read in a server config json file"""
    file = os.path.join(os.path.dirname(__file__), "src/ihme_client/server_config.json")
    with open(file) as f:
        return json.load(f)


config_command = "jobmon update_config --web_service_fqdn {host} --web_service_port {port}"


class PreDevelopCommand(develop):
    """Pre-installation for development mode."""
    def run(self):
        config_params = read_config()
        check_output(config_command.format(**config_params).split())
        develop.run(self)


class PreInstallCommand(install):
    """Pre-installation for installation mode."""
    def run(self):
        config_params = read_config()
        check_output(config_command.format(**config_params).split())
        install.run(self)


setup(
    cmdclass={
        'develop': PreDevelopCommand,
        'install': PreInstallCommand,
    }
)
