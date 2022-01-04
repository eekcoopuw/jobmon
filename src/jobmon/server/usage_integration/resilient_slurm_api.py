from typing import Any

from slurm_rest import ApiClient  # type: ignore
from slurm_rest.api import SlurmApi  # type: ignore
from slurm_rest.rest import ApiException  # type: ignore
from slurm_rest.models import V0036JobSubmission  # type: ignore  # noqa: I100
from tenacity import retry, retry_if_exception_type, stop_after_delay, wait_exponential


class ResilientSlurmApi(SlurmApi):
    """A wrapper class for the Slurm API to handle intermittent 502 errors.

    All routes will be retried for
    """

    def __init__(self, api_client: ApiClient) -> None:
        """Initialize the API object."""
        super().__init__(api_client)

    @retry(
        retry=retry_if_exception_type(ApiException),
        wait=wait_exponential(max=90),
        stop=stop_after_delay(180),
    )
    def slurmctld_get_jobs(self) -> Any:
        """Get running job IDs from the rest API."""
        return super().slurmctld_get_jobs()

    @retry(
        retry=retry_if_exception_type(ApiException),
        wait=wait_exponential(max=90),
        stop=stop_after_delay(180),
    )
    def slurmctl_submit_job(self, job_submission: V0036JobSubmission) -> Any:
        """Submit a batch job to the scheduler."""
        return super().slurmctld_submit_job(job_submission)

    @retry(
        retry=retry_if_exception_type(ApiException),
        wait=wait_exponential(max=90),
        stop=stop_after_delay(180),
    )
    def slurmctld_cancel_job(self, job_id: int) -> Any:
        """Cancel jobs by ID."""
        return super().slurmctld_cancel_job(job_id)

    @retry(
        retry=retry_if_exception_type(ApiException),
        wait=wait_exponential(max=90),
        stop=stop_after_delay(180),
    )
    def slurmdbd_get_job(self, job_id: int) -> Any:
        """Get stats for a completed job from the accounting database."""
        return super().slurmdbd_get_job(job_id)
