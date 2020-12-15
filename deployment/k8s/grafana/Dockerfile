FROM grafana/grafana:7.0.3

# Install Plugin(s)
# Specific version, because current version 7.0.0 is broken 2020-06-12
# https://github.com/grafana/influxdb-flux-datasource/issues/101
RUN grafana-cli plugins install grafana-influxdb-flux-datasource 5.4.1

# Just add some files to the grafana image
ADD k8s/grafana/provisioning /etc/grafana/provisioning