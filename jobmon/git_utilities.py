import subprocess


def get_commit_hash(dir="."):
    """get the git commit hash for a given directory

    Args:
        dir (string): which directory to get the git hash for. defaults to
            current directory

    Returns:
        git commit hash
    """
    cmd = [
        'git',
        '--git-dir=%s/.git' % dir,
        '--work-tree=%s',
        'rev-parse',
        'HEAD']
    return subprocess.check_output(cmd).strip()


def get_branch(dir="."):
    """get the git branch for a given directory

    Args:
        dir (string): which directory to get the git commit for. defaults to
            current directory

    Returns:
        git commit branch
    """
    cmd = [
        'git',
        '--git-dir=%s/.git' % dir,
        '--work-tree=%s',
        'rev-parse',
        '--abbrev-ref',
        'HEAD']
    return subprocess.check_output(cmd).strip()


def git_dict(dir="."):
    """get a dictionary of the git branch and hash for given directory.

    Args:
        dir (string): which directory to get the dictionary for

    Returns:
        dictionary
    """
    branch = get_branch(dir)
    commit = get_commit_hash(dir)
    return {'branch': branch, 'commit': commit}
