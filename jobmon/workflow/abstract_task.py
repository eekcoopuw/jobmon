import logging
import hashlib

logger = logging.getLogger(__name__)


class AbstractTask(object):
    """
    The root of the Task class tree.
    All tasks have a set of upstream and a set of downstream tasks.

    Executable jobs (in release Dugong) have a jobmon.Job, which is executed on
    the SGE cluster.
    External Tasks (fin release Frog) do not have Jobs, because they represent
    input tasks that are "givens" and cannot be executed.
    """

    ILLEGAL_SPECIAL_CHARACTERS = r"/\\'\""

    @staticmethod
    def is_valid_sge_job_name(name):
        """
        If the name is invalid it will raises an exception. The list of illegal
        characters might not be complete, I could not find an official list.

        TBD This should probably be moved to the cluster_utils package

        Must:
          - Not be null or the empty string
          - being with a digit
          - contain am illegal character

        Args:
            name:

        Returns:
            True (or raises)

        Raises:
            ValueError: if the name is not valid.
        """

        if not name:
            raise ValueError("name cannot be None or empty")
        elif name[0].isdigit():
            raise ValueError("name cannot begin with a digit, saw: '{}'"
                             .format(name[0]))
        elif any(e in name for e in AbstractTask.ILLEGAL_SPECIAL_CHARACTERS):
            raise ValueError("name contains illegal special character, "
                             "illegal characters are: '{}'"
                             .format(AbstractTask.ILLEGAL_SPECIAL_CHARACTERS))

        return True

    def __init__(self, command):
        """
        Create a task

        Args
         command: the unique command for this Task, also readable by humans.
         Should include all parameters.
         Two Tasks are equal (__eq__) iff they have the same command

         Raise:
           ValueError: If the hashed command is not allowed as an SGE job name;
           see is_valid_sge_job_name
        """
        # Hash must be an integer, in order for it to be returned by __hash__
        self.hash = int(hashlib.sha1(command.encode('utf-8')).hexdigest(), 16)
        # Names of sge jobs can't start with a numeric.
        self.hash_name = "task_{}".format(self.hash)
        AbstractTask.is_valid_sge_job_name(self.hash_name)

        self.upstream_tasks = set()
        self.downstream_tasks = set()

    def __eq__(self, other):
        """
        Two tasks are equal if they have the same hash_name.
        Needed for sets
        """
        return self.hash == other.hash

    def __hash__(self):
        """
        Logic must match __eq__
        """
        return self.hash

    def needs_to_execute(self):
        """
        Should this Task be run? True if not done.

        Abstract

        Returns:
            boolean: true if this must be executed, False if not
        """
        raise NotImplementedError()

    def all_upstreams_complete(self):
        """
        Are all my upstreams complete?

        Abstract
        """
        raise NotImplementedError()

    def add_upstream(self, ancestor):
        """
        Add an upstream (ancestor) Task. This has Set semantics, an upstream
        task will only be added once. Symmetrically, this method also adds this
        Task as a downstream on the ancestor.
        """
        self.upstream_tasks.add(ancestor)
        # avoid endless recursion, set directly
        ancestor.downstream_tasks.add(self)

    def add_downstream(self, descendent):
        """
        Add an downstream (ancestor) Task. This has Set semantics, a downstream
        task will only be added once. Symmetrically, this method also adds this
        Task as an upstream on the ancestor.
        """
        self.downstream_tasks.add(descendent)
        # avoid endless recursion, set directly
        descendent.upstream_tasks.add(self)
