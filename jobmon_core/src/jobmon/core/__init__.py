import importlib
import pkg_resources
import pkgutil

__version__ = pkg_resources.get_distribution("jobmon_core").version

def _get_config_file_from_installer_plug_in() -> str:
    """Return the default configuration file path from the installer plug-in."""
    # if the installer plugin exists, use the config file form the plugin
    plugins = [
        plugin_name
        for finder, plugin_name, ispkg in pkgutil.iter_modules()
        if plugin_name.startswith("jobmon_installer")
    ]
    if len(plugins) == 1:
        plugin_name = plugins[0]
        print(f"Found one plugin: {plugin_name}. Installing config from plugin.")
        module = importlib.import_module(plugin_name)
        get_config_file = getattr(module, "get_config_file")
        filepath = get_config_file()
        return filepath
    elif len(plugins) > 1:
        raise(
            "Found multiple plugins while installing config from plugin, but only one "
            f'is allowed. Got "{plugins}"'
        )


CONFIG_FILE_FROM_INSTALLER_PLUGIN = _get_config_file_from_installer_plug_in()

