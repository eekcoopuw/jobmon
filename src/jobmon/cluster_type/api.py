"""Mapping to different executors depending on which is being used."""
import importlib

from typing import Any, Dict

from jobmon.exceptions import UnregisteredClusterType


known_clusters: Dict[str, str] = {
    "dummy": "jobmon.cluster_type.dummy",
    "multiprocess": "jobmon.cluster_type.multiprocess",
    "sequential": "jobmon.cluster_type.sequential"
}


_plugins: Dict[str, Any] = {}


def register_cluster_plugin(cluster_type_name: str, plugin_module_path: str) -> None:
    known_clusters[cluster_type_name] = plugin_module_path


def _validate_plugin(module: Any):
    # TODO: Scan module to make sure import functions are implemented
    pass


def import_cluster(cluster_type_name: str) -> Any:
    """Get a cluster interface for a given type of cluster.

    Args:
        cluster_type_name: the name of the cluster technology.
    """
    module = _plugins.get(cluster_type_name)
    if module is None:
        try:
            module_path = known_clusters[cluster_type_name]
        except KeyError:
            raise UnregisteredClusterType(
                f"cluster_type_name={cluster_type_name} has not been registered as a plugin "
                "with jobmon. Please use register_cluster_plugin to make jobmon aware of it."
            )
        try:
            module = importlib.import_module(module_path)
            _validate_plugin(module)
            _plugins[cluster_type_name] = module
        except ModuleNotFoundError as e:
            msg = f"Interface not found for cluster_type_name={cluster_type_name}"
            raise ValueError(msg) from e
    return module
