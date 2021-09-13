

# install all client packages
$PYTHON -m pip install ./wheels/jobmon-*.whl
$PYTHON -m pip install ./wheels/jobmon_uge-*.whl

# configure client
# 10.158.146.73
$PREFIX/bin/jobmon update_config --web_service_fqdn $WEB_SERVER_FQDN --web_service_port $WEB_SERVER_PORT

