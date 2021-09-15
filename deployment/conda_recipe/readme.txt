To build ihme_client recipe:
1) export env variables:
  a) IHME_CLIENT - the version number for this client
  b) WEB_SERVER_FQDN - the service domain for this client build
  c) WEB_SERVER_PORT - the service port for this client build
  d) JOBMON_VERSION - version specifier for core jobmon
  e) JOBMON_UGE_VERSION - version specifier for jobmon_uge
2) conda build ihme_client -c conda-forge
