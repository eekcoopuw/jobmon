import os
import sys
import shutil


def valid_full_version(vers_str):
    return (('post' not in vers_str) and
            ('current' not in vers_str) and
            ('latest-pre' not in vers_str))


def pre_vers_tuple(vers_string):
    post_split = vers_string.split(".post.")
    full_vers = tuple(map(int, post_split[0].split(".")))

    # Ensure full_vers conforms to three part MAJOR.MINOR.PATCH semver
    full_vers = tuple(0 for i in range(3-len(full_vers))) + full_vers
    pre_vers = full_vers + (int(post_split[1].replace("dev", "")), )
    return pre_vers


def vers_tuple(vers_string):
    print("vers_string:" + vers_string)
    def my_int(s):
        # We have old testing release contains non number chars in the version name. Ignor them.
        try:
            return int(s)
        except:
            return 0
    return tuple(map(my_int, vers_string.split(".")))


def max_pre_version(versioned_doc_dir):
    versions = os.listdir(os.path.abspath(
        os.path.expanduser(versioned_doc_dir)))
    pre_versions = [vers for vers in versions if 'post' in vers]
    if len(pre_versions) == 0:
        return None
    max_pre_tuple = max([pre_vers_tuple(pv) for pv in pre_versions])
    return max_pre_tuple


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


def docs(name, pydir, odir):
    """Copies the built html docs in {pydir}/build/html to a destination
    directory (odir) that is presumably web-accessible"""
    pydir = os.path.abspath(pydir)
    pyddir = os.path.join(os.path.join(pydir, 'build/html'))

    sys.path.insert(1, pydir)
    os.chdir(pydir)
    import versioneer

    odir = os.path.abspath(odir)
    vers = versioneer.get_version().split("+")[0]
    odir = os.path.join(odir, name, vers)
    try:
        os.makedirs(odir)
        shutil.copytree(pyddir, odir)
    except:
        shutil.rmtree(odir)
        shutil.copytree(pyddir, odir)


def current_edge_symlinks(index_dir):
    """Creates symlinks to edge and current within an index of versioned
    folders"""
    index_dir = os.path.abspath(os.path.expanduser(index_dir))
    max_fv = max_full_version(index_dir)
    max_pre = max_pre_version(index_dir)

    if max_fv:
        max_fv_fn = ".".join(map(str, max_fv))
        link_dest = "{}/current".format(index_dir)
        if os.path.islink(link_dest):
            os.remove(link_dest)
        os.symlink(max_fv_fn, link_dest)
    if max_pre:
        max_pre_fn = "{}.post.dev{}".format(".".join(map(str, max_pre[:3])),
                                            max_pre[3])
        link_dest = "{}/latest-pre".format(index_dir)
        if os.path.islink(link_dest):
            os.remove(link_dest)
        os.symlink(max_pre_fn, link_dest)


if __name__ == "__main__":
    index_dir = sys.argv[1]
    current_edge_symlinks(index_dir)
