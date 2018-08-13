from flask import g

# NOTE: imports of config and the specific services are INTENTIONALLY put
# inside these functions, because module level imports cause config issues
# related to a load order bug. These issues will be sorted out with GBDSCI-1089


def run_jsm(client_cfg_opts, server_cfg_opts):
    from jobmon.server.config import ServerConfig
    from jobmon.client.config import ClientConfig
    global the_client_config
    the_client_config = ClientConfig(**client_cfg_opts)
    global the_server_config
    the_server_config = ServerConfig(**server_cfg_opts)

    from jobmon.server.services.job_state_manager.job_state_manager import app
    with app.app_context():
        g.the_server_config = the_server_config
        g.the_client_config = the_client_config

    app.run(host="0.0.0.0", port=the_client_config.jsm_port, debug=True,
            use_reloader=False, use_evalex=False, threaded=False)


def run_jqs(client_cfg_opts, server_cfg_opts):
    from jobmon.server.config import ServerConfig
    from jobmon.client.config import ClientConfig
    global the_client_config
    the_client_config = ClientConfig(**client_cfg_opts)
    global the_server_config
    the_server_config = ServerConfig(**server_cfg_opts)

    from jobmon.server.services.job_query_server.job_query_server import app
    with app.app_context():
        g.the_server_config = the_server_config
        g.the_client_config = the_client_config

    app.run(host="0.0.0.0", port=the_client_config.jqs_port, debug=True,
            use_reloader=False, use_evalex=False, threaded=False)
