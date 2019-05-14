import os
import sys

import pytest

from jobmon.client.utils import (confirm_correct_perms,
                                 _get_ssh_permission_dict,
                                 _setup_keyfile)
from jobmon.exceptions import UnsafeSSHDirectory


# set up ssh keys to spec if you don't have them
_setup_keyfile()


ref = {'~': ['700', '701', '704', '705', '710', '711', '714', '715', '740',
             '741', '744', '745', '750', '751', '754', '755'],
       '~/.ssh': ['700'],
       '~/.ssh/jobmonauto_id_rsa': ['600'],
       '~/.ssh/authorized_keys': ['644', '600'],
       '~/.ssh/authorized_keys2': ['644', '600']}


def convert_perms(perms):
    if sys.version_info.major == 3:
        return int("0o" + perms, 8)
    else:
        return int("0" + perms, 8)


def test_permission_dict():
    perms_dict = _get_ssh_permission_dict()
    for key in ref.keys():
        perms_dict_key = os.path.realpath(os.path.expanduser(key))
        assert ref[key] == perms_dict[perms_dict_key]


def test_incorrect_h_permissions(tmpdir):
    test_dct = ref.copy()
    new_dir = str(tmpdir)
    test_dct[new_dir] = ref["~"]
    del test_dct["~"]
    os.chmod(new_dir, convert_perms("666"))
    with pytest.raises(UnsafeSSHDirectory):
        confirm_correct_perms(test_dct)


def test_no_ssh_dir_is_okay(tmpdir):
    test_dct = {}
    new_dir = str(tmpdir)
    test_dct[new_dir] = ref["~"]
    os.chmod(new_dir, convert_perms("755"))
    confirm_correct_perms(test_dct)


def test_correct_file_permission(tmpdir):
    test_dct = {}
    new_dir = str(tmpdir) + "/foo"
    for key in ref.keys():
        new_key = key.replace("~", new_dir)
        test_dct[new_key] = ref[key]

        if isinstance(ref[key], list):
            mode = convert_perms(ref[key][-1])
        else:
            mode = convert_perms(ref[key])
        os.makedirs(new_key, mode=mode)
    confirm_correct_perms(test_dct)
