import os

# NOTE: imports of config and the specific services are INTENTIONALLY put
# inside these functions, because module level imports cause config issues
# related to a load order bug. These issues will be sorted out with GBDSCI-1089


def run_jsm(port):
    from jobmon.services.job_state_manager import app
    app.run(host="0.0.0.0", port=port, debug=True,
            use_reloader=False, use_evalex=False, threaded=False)


def run_jqs(port):
    from jobmon.services.job_query_server import app
    app.run(host="0.0.0.0", port=port, debug=True,
            use_reloader=False, use_evalex=False, threaded=False)
