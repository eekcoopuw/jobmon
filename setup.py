import os
from setuptools import setup
from codecs import open
import versioneer
from setuptools import setup
try:
    from autowrap import wrapper
    HAS_AUTOWRAP = True
except:
    HAS_AUTOWRAP = False
import subprocess
import re
import os
import sys


here = os.path.abspath(os.path.dirname(__file__))
package_dir = os.path.join(here, 'jobmon)')

# Make package importable so wrappers can be generated before true installation
sys.path.insert(0, package_dir)

# this could potentially be looked up using an environment variable...
wrapper_build_dir = os.path.join(here, 'build')
wrapper_cfg = os.path.join(here, 'autowrap.cfg')

# Extend the build_py command to create wrappers
vcmds = versioneer.get_cmdclass()
_build_py = vcmds['build_py']


class WrapperInstall(_build_py):
    def run(self):

        # get name of current environment
        if HAS_AUTOWRAP:
            stdout = subprocess.check_output("conda info --envs", shell=True)
            env_name, env_path = re.findall('\n(\S*)\s*\*\s*(\S*)\n',
                                            stdout,
                                            re.MULTILINE)[0]

            # make wrapper directory
            if not os.path.exists(wrapper_build_dir):
                os.makedirs(wrapper_build_dir)

            # gen wrappers
            wrappers = wrapper.generate(
                wrapper_cfg,
                wrapper_build_dir,
                env_path)

            # TODO: move wrappers
            for wf in wrappers:
                pass

        # run normal install
        _build_py.run(self)


cmds = {}
cmds['sdist'] = vcmds['sdist']
cmds['version'] = vcmds['version']
cmds['build_py'] = WrapperInstall

# Get the long description from the README file
with open(os.path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    version=versioneer.get_version(),

    cmdclass=cmds,
    name='jobmon',
    use_scm_version=True,
    description="Database-backed job monitor",
    long_description=long_description,
    url='',
    author='',
    author_email='',
    install_requires=[
        'pandas',
        'sqlalchemy',
        'numpy',
        'pymysql',
        'pyzmq',
        'setuptools_scm',
        'pyyaml'],
    package_data={'jobmon': ['*.sh']},
    packages=['jobmon'],
    scripts=["bin/env_submit_master.sh",
             "bin/submit_master.sh",
             "bin/launch_central_monitor.py",
             "bin/monitored_job.py"]
    )
