"""Inteface definition for jobmon executor plugins."""
from __future__ import annotations
import importlib
import pkgutil

from typing import Any, Type, Dict

from jobmon.core.cluster_protocol import ClusterDistributor, ClusterQueue, ClusterWorkerNode
import jobmon.plugins


def _iter_namespace(ns_pkg):
    # Specifying the second argument (prefix) to iter_modules makes the
    # returned name an absolute name instead of a relative one. This allows
    # import_module to work without having to do additional modification to
    # the name.
    return pkgutil.iter_modules(ns_pkg.__path__, ns_pkg.__name__ + ".")


class ClusterType:
    _interface = [
        "get_cluster_queue_class",
        "get_cluster_distributor_class",
        "get_cluster_worker_node_class",
    ]

    _cache: Dict[str, ClusterType] = {}

    def __new__(cls, *args: str, **kwds: str) -> ClusterType:
        key = args[0] if args else kwds["cluster_type_name"]
        inst = cls._cache.get(key, None)
        if inst is None:
            inst = super(ClusterType, cls).__new__(cls)
            inst.__init__(key)  # type: ignore
            cls._cache[key] = inst
        return inst

    def __init__(self, cluster_type_name: str) -> None:
        """Initialization of ClusterType object."""
        self.cluster_type_name = cluster_type_name

        self._plugins = {
            name: importlib.import_module(name)
            for finder, name, ispkg
            in _iter_namespace(jobmon.plugins)
        }
        self._package_location = ""

    def get_plugin(self, name: str) -> Any:
        """Get a cluster interface for a given type of cluster.

        Args:
            plugin_module_path: path to the plugin module e.g. {"slurm_rest_host":
            "https://api.cluster.ihme.washington.edu", "slurmtool_token_host":
            "https://slurmtool.ihme.washington.edu/api/v1/token/"}
        """
        module = self._plugins.get(f"jobmon.plugins.{name}")
        if module is not None:
            msg = ""
            for func in self._interface:
                if not hasattr(module, func):
                    msg += f"Required function {func} missing from plugin interface. \n"
            if msg:
                raise AttributeError(f"Invalid jobmon plugin {name}" + msg)
        else:
            msg = f"Interface not found for cluster_type_name={name}"
            raise ValueError(msg)
        return module

    @property
    def plugin(self) -> Any:
        """If the cluster is bound, return the cluster interface for the type of cluster."""
        return self.get_plugin(self.cluster_type_name)

    @property
    def cluster_queue_class(self) -> Type[ClusterQueue]:
        return self.plugin.get_cluster_queue_class()

    @property
    def cluster_distributor_class(self) -> Type[ClusterDistributor]:
        return self.plugin.get_cluster_distributor_class()

    @property
    def cluster_worker_node_class(self) -> Type[ClusterWorkerNode]:
        return self.plugin.get_cluster_worker_node_class()
