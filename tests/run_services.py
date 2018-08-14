import os
import sys
from signal import signal, SIGTERM
# NOTE: imports of config and the specific services are INTENTIONALLY put
# inside these functions, because module level imports cause config issues
# related to a load order bug. These issues will be sorted out with GBDSCI-1089


def shutdown_session(exception=None):
    from jobmon.server.database import ScopedSession
    ScopedSession.remove()
    sys.exit(0)


def run_jsm():
    from jobmon.client.the_client_config import get_the_client_config
    from jobmon.server.services.job_state_manager.app import create_app

    app = create_app(os.environ['host'], os.environ['conn_str'])
    app.run(host="0.0.0.0", port=get_the_client_config().jsm_port, debug=True,
            use_reloader=False, use_evalex=False, threaded=False)
    signal(SIGTERM, shutdown_session)


def run_jqs():
    from jobmon.client.the_client_config import get_the_client_config
    from jobmon.server.services.job_query_server.app import create_app

    app = create_app(os.environ['host'], os.environ['conn_str'])
    app.run(host="0.0.0.0", port=get_the_client_config().jqs_port, debug=True,
            use_reloader=False, use_evalex=False, threaded=False)
    signal(SIGTERM, shutdown_session)
