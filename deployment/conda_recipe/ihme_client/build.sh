# install all client packages
pip install ./wheels/jobmon-*.whl
pip install ./wheels/jobmon_uge-*.whl
pip install ./wheels/jobmon_slurm-*.whl

# configure client
# 10.158.146.73
$PREFIX/bin/jobmon update_config --web_service_fqdn $WEB_SERVICE_FQDN --web_service_port $WEB_SERVICE_PORT
