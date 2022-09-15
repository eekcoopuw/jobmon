import os
from typing import Any, Dict, Optional

from jobmon.requester import Requester


def add_task_attributes(attr_dict: Dict[str, Any], task_id: Optional[int] = None) -> None:
    """Add task attributes to database.

    Args:
        attr_dict: Key/value pairs to store as task attributes. must be coercable to json.
        task_id: The task id to associate the attributes to. If not provided, will check the
            shell environment for a task id. Otherwise will raise a ValueError

    Raises: ValueError, TypeError
    """

    try:
        if task_id is None:
            task_id = int(os.environ["JOBMON_TASK_ID"])
    except KeyError:
        raise ValueError(
            "JOBMON_TASK_ID not found in environment. Must specify task_id if not calling in "
            "the context of a task."
        )

    requester = Requester.from_defaults()
    requester.send_request(app_route=f"/task/{task_id}/attributes", data=attr_dict)
