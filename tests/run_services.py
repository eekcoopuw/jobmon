import os
# NOTE: imports of config and the specific services are INTENTIONALLY put
# inside these functions, to comply to a application_factory Flask pattern
# and encapsulate configs


def run_jsm():
    from jobmon.client.the_client_config import get_the_client_config
    from jobmon.server.services.job_state_manager.app import create_app

    app = create_app(os.environ['JOBMON_HOST'], os.environ['CONN_STR'])
    app.run(host="0.0.0.0", port=get_the_client_config().jsm_port, debug=True,
            use_reloader=False, use_evalex=False, threaded=False)


def run_jqs():
    from jobmon.client.the_client_config import get_the_client_config
    from jobmon.server.services.job_query_server.app import create_app

    app = create_app(os.environ['JOBMON_HOST'], os.environ['CONN_STR'])
    app.run(host="0.0.0.0", port=get_the_client_config().jqs_port, debug=True,
            use_reloader=False, use_evalex=False, threaded=False)
