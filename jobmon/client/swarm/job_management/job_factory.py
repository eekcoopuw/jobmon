from http import HTTPStatus as StatusCodes
import json
import logging

from jobmon.client import shared_requester
from jobmon.exceptions import InvalidResponse
from jobmon.models.attributes.constants import job_attribute
from jobmon.models.job import Job


logger = logging.getLogger(__name__)


class JobFactory(object):

    def __init__(self, dag_id, requester=shared_requester):
        self.dag_id = dag_id
        self.requester = requester

    def create_job(self, command, jobname, job_hash, slots=None,
                   num_cores=None, mem_free=2, max_attempts=1,
                   max_runtime_seconds=None, context_args=None, tag=None,
                   queue=None, j_resource=False):
        """
        Create a job entry in the database.

        Args:
            command (str): the command to run
            jobname (str): name of the job
            job_hash (str): hash of the job
            slots (int): Number of slots to request from SGE
            mem_free (int): Number of GB of memory to request from SGE
            max_attempts (int): Maximum # of attempts before sending the job to
                ERROR_FATAL state
            max_runtime_seconds (int): Maximum runtime in seconds of a single
                job_instance before killing and marking that instance as failed
            context_args (dict): Additional arguments to be sent to the command
                builders
            tag (str, default None): a group identifier
            queue (str): queue of cluster nodes to submit this task to. Must be
                a valid queue, as defined by "qconf -sql"
            j_resource (bool): whether or not this job is using the j drive
        """
        if not context_args:
            context_args = json.dumps({})
        else:
            context_args = json.dumps(context_args)
        rc, response = self.requester.send_request(
            app_route='/job',
            message={'dag_id': self.dag_id,
                     'name': jobname,
                     'job_hash': job_hash,
                     'command': command,
                     'context_args': context_args,
                     'slots': slots,
                     'num_cores': num_cores,
                     'mem_free': mem_free,
                     'max_attempts': max_attempts,
                     'max_runtime_seconds': max_runtime_seconds,
                     'tag': tag,
                     'queue': queue,
                     'j_resource': j_resource
                     },
            request_type='post')
        if rc != StatusCodes.OK:
            raise InvalidResponse(
                "{rc}: Could not create_job {e}".format(rc=rc, e=jobname))
        return Job.from_wire(response['job_dct'])

    def queue_job(self, job_id):
        """Transition a job to the Queued for Instantiation status in the db

        Args:
            job_id (int): the id of the job to be queued
        """
        rc, _ = self.requester.send_request(
            app_route='/job/{}/queue'.format(job_id),
            message={},
            request_type='post')
        if rc != StatusCodes.OK:
            raise InvalidResponse("{rc}: Could not queue_job".format(rc))
        return rc

    def reset_jobs(self):
        """Reset all incomplete jobs of a dag_id, identified by self.dag_id"""
        rc, _ = self.requester.send_request(
            app_route='/task_dag/{}/reset_incomplete_jobs'.format(self.dag_id),
            message={},
            request_type='post')
        if rc != StatusCodes.OK:
            raise InvalidResponse("{rc}: Could not reset jobs".format(rc))
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
        user_cant_config = [job_attribute.WALLCLOCK, job_attribute.CPU,
                            job_attribute.IO, job_attribute.MAXRSS]
        if attribute_type in user_cant_config:
            raise ValueError(
                "Invalid attribute configuration for {} with name: {}, user "
                "input not used to configure attribute value".format(
                    attribute_type, type(attribute_type).__name__))
        elif not isinstance(attribute_type, int):
            raise ValueError("Invalid attribute_type: {}, {}"
                             .format(attribute_type,
                                     type(attribute_type).__name__))
        elif (not attribute_type == job_attribute.TAG and not int(value))\
                or (attribute_type == job_attribute.TAG and
                    not isinstance(value, str)):
            raise ValueError("Invalid value type: {}, {}"
                             .format(value,
                                     type(value).__name__))
        else:
            rc, job_attribute_id = self.requester.send_request(
                app_route='/job_attribute',
                message={'job_id': str(job_id),
                         'attribute_type': str(attribute_type),
                         'value': str(value)},
                request_type='post')
            return job_attribute_id
