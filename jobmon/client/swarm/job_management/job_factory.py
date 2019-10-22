from http import HTTPStatus as StatusCodes

from jobmon.client import shared_requester
from jobmon.exceptions import InvalidResponse
from jobmon.models.attributes.constants import job_attribute
from jobmon.client.swarm.job_management.swarm_job import SwarmJob
from jobmon.client.client_logging import ClientLogging as logging


logger = logging.getLogger(__name__)


class JobFactory(object):

    def __init__(self, dag_id, requester=shared_requester):
        self.dag_id = dag_id
        self.requester = requester

    def create_job(self, command, jobname, job_hash, tag=None, max_attempts=1):
        """
        Create a job entry in the database.

        Args:
            command (str): the command to run
            jobname (str): name of the job
            job_hash (str): hash of the job
            max_attempts (int): Maximum # of attempts before sending the job to
                ERROR_FATAL state
            tag (str, default None): a group identifier

        """
        logger.info("Create job for dag_id {}".format(self.dag_id))
        rc, response = self.requester.send_request(
            app_route='/job',
            message={'dag_id': self.dag_id,
                     'name': jobname,
                     'job_hash': job_hash,
                     'command': command,
                     'tag': tag,
                     'max_attempts': max_attempts},
            request_type='post')
        if rc != StatusCodes.OK:
            raise InvalidResponse(
                "{rc}: Could not create_job {e}".format(rc=rc, e=jobname))

        return SwarmJob.from_wire(response['job_dct'])

    def queue_job(self, job_id):
        """Transition a job to the Queued for Instantiation status in the db

        Args:
            job_id (int): the id of the job to be queued
        """
        logger.info("Queue job jid {}".format(job_id))
        rc, _ = self.requester.send_request(
            app_route='/job/{}/queue'.format(job_id),
            message={},
            request_type='post')
        if rc != StatusCodes.OK:
            raise InvalidResponse(f"{rc}: Could not queue_job")
        return rc

    def reset_jobs(self):
        """Reset all incomplete jobs of a dag_id, identified by self.dag_id"""
        logger.info("Reset jobs for dag_id {}".format(self.dag_id))
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
        logger.info("Add job attribute for job_id: {jid} attribute_type: {attribute_type} value: {v}".format(
            jid=job_id, attribute_type=attribute_type, v=value
        ))
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
