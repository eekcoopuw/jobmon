"""Creates a single instance of the test database, used for tests in multiprocessing mode."""
import json
from typing import Any, Callable, Union

from filelock import FileLock

from jobmon.test_utils.create_temp_db import create_temp_db


def ephemera_db_instance(
    tmp_path_factory: Any, worker_id: str = "master"
) -> Union[dict, Callable[[], dict]]:
    """Boots exactly one instance of the test ephemera database.

    If tests are run in multiprocessing mode, ensure only one database
    is created.

    Arguments:
        tmp_path_factory: a pytest tmp_path_factory object
        worker_id: a pytest-xdist worker_id fixture

    Returns:
      a dictionary with connection parameters
    """
    if worker_id == "master":
        # not executing with multiple workers, just produce the data and let
        # pytest's fixture caching do its job
        return create_temp_db()

    # get the temp directory shared by all workers
    root_tmp_dir = tmp_path_factory.getbasetemp().parent

    # Only want one instance of the database
    fn = root_tmp_dir / "data.json"
    with FileLock(str(fn) + ".lock"):
        if fn.is_file():
            connection_information = json.loads(fn.read_text())
        else:
            connection_information = create_temp_db()
            fn.write_text(json.dumps(connection_information))
    return connection_information
