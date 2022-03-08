from multiprocessing.pool import ApplyResult
import time

from slurm_rest import ApiClient  # type: ignore
from slurm_rest.api import SlurmApi  # type: ignore
from slurm_rest.rest import ApiException  # type: ignore
from tenacity import retry, retry_if_exception_type, stop_after_delay, wait_exponential


class ResilientSlurmApi(SlurmApi):
    """A wrapper class for the Slurm API to handle intermittent 502 errors.

    All routes will be retried for
    """

    # Treat this class like a singleton
    instance = None
    last_token_refresh_date = 0

    def __init__(self, api_client: ApiClient) -> None:
        """Initialize the API object."""
        super().__init__(api_client)
        ResilientSlurmApi.instance = self
        ResilientSlurmApi.last_token_refresh_date = time.time()

    @retry(
        retry=retry_if_exception_type(ApiException),
        wait=wait_exponential(max=90),
        stop=stop_after_delay(180),
    )
    def slurmdbd_get_job(self, job_id: int) -> ApplyResult:
        """Get stats for a completed job from the accounting database."""
        return super().slurmdbd_get_job(job_id, async_req=True)
