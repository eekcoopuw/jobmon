# NOTE: imports of config and the specific services are INTENTIONALLY put
# inside these functions, to comply to a application_factory Flask pattern
# and encapsulate configs


def run_web_service(service_port, db_host, db_port, db_user, db_pass, db_name):
    from jobmon.server import ServerConfig
    from jobmon.server import create_app

    config = ServerConfig.from_defaults()
    config.db_host = db_host
    config.db_port = db_port
    config.db_user = db_user
    config.db_pass = db_pass
    config.db_name = db_name
    app = create_app(config)

    app.run(host="0.0.0.0", port=service_port, debug=True, use_reloader=False,
            use_evalex=False, threaded=False)
