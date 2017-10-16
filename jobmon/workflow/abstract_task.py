import logging

logger = logging.getLogger(__name__)


class AbstractTask(object):
    """
    Tasks has-a Job, because external tasks have no jobs.
    """

    ILLEGAL_SPECIAL_CHARACTERS = r"/\\'\""

    @staticmethod
    def is_valid_task_name(name, raise_on_error=True):
        """
        Deliberately does not raise an exception - that is for the caller to decide.

        Must:
          Not be null or the empty string
        Args:
            name:
            raise_on_error: If true, raise ValueError itf it is invalid, otherwise retun (False, error_message)

        Returns:
            if raise_on_error is false, then return a Tuple : (True,"") if valid; or ( False, error-msg) if invalid

        """
        error = ""
        if not name:
            error = "name cannot be None or empty"
        elif name[0].isdigit():
            return False, "name cannot begin with a digit, saw: '{}'".format(name[0])
        elif any(e in name for e in AbstractTask.ILLEGAL_SPECIAL_CHARACTERS):
            return False, "name contains illegal special character, illegal characters are: '{}'". \
                format(AbstractTask.ILLEGAL_SPECIAL_CHARACTERS)

        if error:
            if raise_on_error:
                raise ValueError(error)
            else:
                return False, error
        else:
            return True

    def __init__(self, hash_name):
        """
        Create a task

        Args
         hash_name: the unique name for this Task, also readable by homans. Should include all parameters.

         Raise:
           ValueError: If the hash_name is not allowed as an SGE job name,
           ie. cannot start with a digit and cannot use these special characters: / ' " \
        """
        AbstractTask.is_valid_task_name(hash_name)
        self.hash_name = hash_name

        self.upstream_tasks = []
        self.downstream_tasks = []

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
        Add an upstream (ancestor) Task. This has Set semantics, an upstream task will only be added once.
        Symmetrically, this method also adds this Task as a downstream on the ancestor.
        """
        if ancestor not in self.upstream_tasks:
            self.upstream_tasks.append(ancestor)
            ancestor.add_downstream(self)

    def add_downstream(self, descendent):
        """
        Add an downstream (ancestor) Task. This has Set semantics, a downstream task will only be added once.
        Symmetrically, this method also adds this Task as an upstream on the ancestor.
        """
        if descendent not in self.downstream_tasks:
            self.downstream_tasks.append(descendent)
            descendent.add_upstream(self)
