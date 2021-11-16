# Originally copied on 11/16/2021
# from https://stash.ihme.washington.edu/projects/SCDO/repos/deploytools/browse/deploytools/publish.py
import os
import sys
import shutil
import re


def valid_full_version(vers_str) -> bool:
    """
    A valid full version is a string with all numbers delimited by .
    """
    splits = vers_str.split(".")
    for s in splits:
        if s.isnumeric() == False:
            return False
    return True


def vers_tuple(vers_string):
    """
    Return a tuple for vers_string
    """
    print("vers_string:" + vers_string)
    def my_int(s):
        # We have old testing release contains non number chars in the version name. Ignor them.
        try:
            return int(s)
        except:
            return 0
    return tuple(map(my_int, vers_string.split(".")))


def max_full_version(versioned_doc_dir):
    """Assumes versioned_doc_dir contains folders whose names correspond to a
    semantic version number, and returns the maximum release version number
    present as a tuple"""
    versions = os.listdir(os.path.abspath(
        os.path.expanduser(versioned_doc_dir)))
    full_versions = [vers for vers in versions if valid_full_version(vers)]
    if len(full_versions) == 0:
        return None
    max_vers_tuple = max([vers_tuple(fv) for fv in full_versions])
    return max_vers_tuple


def current_edge_symlinks(index_dir):
    """Creates symlinks to edge and current within an index of versioned
    folders"""
    index_dir = os.path.abspath(os.path.expanduser(index_dir))
    max_fv = max_full_version(index_dir)

    if max_fv:
        max_fv_fn = ".".join(map(str, max_fv))
        link_dest = "{}/current".format(index_dir)
        if os.path.islink(link_dest):
            os.remove(link_dest)
        os.symlink(max_fv_fn, link_dest)


if __name__ == "__main__":
    index_dir = sys.argv[1]
    current_edge_symlinks(index_dir)
