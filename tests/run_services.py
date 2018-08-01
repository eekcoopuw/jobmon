import os

# NOTE: imports of config and the specific services are INTENTIONALLY put
# inside these functions, because module level imports cause config issues
# related to a load order bug. These issues will be sorted out with GBDSCI-1089


def run_jsm(rcfile, conn_str):
    os.environ['JOBMON_CONFIG'] = rcfile
    from jobmon.config import config
    config.conn_str = conn_str

    from jobmon.services.job_state_manager import app
    app.run(host="0.0.0.0", port=config.jsm_port, debug=True,
            use_reloader=False, use_evalex=False, threaded=False)


def run_jqs(rcfile, conn_str):
    os.environ['JOBMON_CONFIG'] = rcfile
    from jobmon.config import config
    config.conn_str = conn_str

    from jobmon.services.job_query_server import app
    app.run(host="0.0.0.0", port=config.jqs_port, debug=True,
            use_reloader=False, use_evalex=False, threaded=False)
