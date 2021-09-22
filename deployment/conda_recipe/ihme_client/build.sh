
# install all client packages
$PYTHON -m pip install ./wheels/jobmon-*.whl
$PYTHON -m pip install ./wheels/jobmon_uge-*.whl
$PYTHON -m pip install ./wheels/jobmon_slurm-*.whl
$PYTHON -m pip install "https://artifactory.ihme.washington.edu/artifactory/api/pypi/pypi-shared/slurm-rest/1.0.0/slurm-rest-1.0.0.tar.gz#sha256=7343c597d921273db73c8e74f64e87ef622e537b88c94b078431cded3232825a"

# configure client
# 10.158.146.73
$PREFIX/bin/jobmon update_config --web_service_fqdn $WEB_SERVICE_FQDN --web_service_port $WEB_SERVICE_PORT

