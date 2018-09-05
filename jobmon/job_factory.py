import json
import logging

from jobmon import requester
from jobmon.exceptions import InvalidResponse, ReturnCodes
from jobmon.config import config
from jobmon.models import Job
from jobmon.attributes.constants import job_attribute

logger = logging.getLogger(__name__)


class JobFactory(object):

    def __init__(self, dag_id):
        self.dag_id = dag_id
        self.requester = requester.Requester(config.jm_rep_conn)

    def create_job(self, command, jobname, job_hash, slots=1,
                   mem_free=2, max_attempts=1, max_runtime=None,
                   context_args=None, tag=None):
        """
        Create a job entry in the database.

        Args:
            command (str): the command to run
            jobname (str): name of the job
            job_hash (str): hash of the job
            slots (int): Number of slots to request from SGE
            mem_free (int): Number of GB of memory to request from SGE
            max_attmpets (int): Maximum # of attempts before sending the job to
                ERROR_FATAL state
            max_runtime (int): Maximum runtime of a single job_instance before
                killing and marking that instance as failed
            context_args (dict): Additional arguments to be sent to the command
                builders
            tag (str, default None): a group identifier
        """
        if not context_args:
            context_args = json.dumps({})
        else:
            context_args = json.dumps(context_args)
        rc, job_dct = self.requester.send_request({
            'action': 'add_job',
            'kwargs': {'dag_id': self.dag_id,
                       'name': jobname,
                       'job_hash': job_hash,
                       'command': command,
                       'context_args': context_args,
                       'slots': slots,
                       'mem_free': mem_free,
                       'max_attempts': max_attempts,
                       'max_runtime': max_runtime,
                       'tag': tag
                       }
        })
        if rc != ReturnCodes.OK:
            raise InvalidResponse(
                "{rc}: Could not create_job {e}".format(rc=rc,
                                                        e=job_dct['job_id']))
        return Job.from_wire(job_dct)

    def queue_job(self, job_id):
        rc = self.requester.send_request({
            'action': 'queue_job',
            'kwargs': {'job_id': job_id}
        })
        if rc[0] != ReturnCodes.OK:
            raise InvalidResponse("{rc}: Could not queue_job".format(rc=rc))
        return rc

    def reset_jobs(self):
        rc = self.requester.send_request({
            'action': 'reset_incomplete_jobs',
            'kwargs': {'dag_id': self.dag_id}
        })
        if rc[0] != ReturnCodes.OK:
            raise InvalidResponse("{rc}: Could not reset jobs".format(rc=rc))
        return rc

    def add_job_attribute(self, job_id, attribute_type, value):
        """
        Create a job attribute entry in the database.

        Args:
            job_id (int): id of job to attach attribute to
            attribute_type (int): attribute_type id from
                                  job_attribute_type table
            value (int): value associated with attribute

        Raises:
            ValueError: If the args are not valid or if the
                        attribute is used for usage data and
                        cannot be configured on the user side.
                        attribute_type should be int and
                        value should be convertible to int
                        or be string for TAG attribute
        """
        user_cant_config = [job_attribute.WALLCLOCK, job_attribute.CPU, job_attribute.IO, job_attribute.MAXRSS]
        if attribute_type in user_cant_config:
            raise ValueError(
                "Invalid attribute configuration for {} with name: {}, user input not used to configure attribute value"
                    .format(attribute_type, type(attribute_type).__name__))
        elif not isinstance(attribute_type, int):
            raise ValueError("Invalid attribute_type: {}, {}"
                             .format(attribute_type,
                                     type(attribute_type).__name__))
        elif (not attribute_type == job_attribute.TAG and not int(value))\
                or (attribute_type == job_attribute.TAG
                    and not isinstance(value, str)):
            raise ValueError("Invalid value type: {}, {}"
                             .format(value,
                                     type(value).__name__))
        else:
            rc, job_attribute_id = self.requester.send_request({
                'action': 'add_job_attribute',
                'kwargs': {'job_id': job_id,
                           'attribute_type': attribute_type,
                           'value': value}
            })
            return job_attribute_id
