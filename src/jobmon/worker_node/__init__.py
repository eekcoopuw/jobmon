
try:
    from jobmon.worker_node.worker_node_config import WorkerNodeConfig
    wnc = WorkerNodeConfig.from_defaults()
except SystemExit:
    import importlib
    import pkgutil
    from jobmon.exceptions import ConfigError

    print("Jobmon client not configured. Attempting to install configuration for plugin.")
    configured = False

    # try and import any installers
    plugins = [
        plugin_name
        for finder, plugin_name, ispkg
        in pkgutil.iter_modules()
        if plugin_name.startswith('jobmon_installer')
    ]
    if len(plugins) == 1:
        plugin_name = plugins[0]
        print(f"Found one plugin: {plugin_name}")
        module = importlib.import_module(plugin_name)
        config_installer = getattr(module, "install_config")
        config_installer()
        try:
            wnc = WorkerNodeConfig.from_defaults()
            print(f"Successfully configured jobmon to access web service at {wnc.url}")
            configured = True
        except SystemExit:
            pass

    if not configured:
        raise ConfigError("Client not configured to access server. Please use jobmon_config "
                          "command to specify which jobmon server you want to use.")
