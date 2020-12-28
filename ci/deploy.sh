
# jobmon_ini_file()
# {
#     METALLB_IP_POOL="$1"

#     kubectl -n metallb-system \
#       get configmap config -o "jsonpath={.data.config}" | \
#       grep -A 4 ${METALLB_IP_POOL} > metallb_ip.cfg

#     TARGET_IP=$(cat metallb_ip.cfg | \
#                 grep "\- [0-9].*/[0-9]*" | \
#                 sed -e "s/  - \(.*\)\/32/\1/")
#     echo "
#     [client]
#     web_service_fqdn=$TARGET_IP
#     web_service_port=80
#     " > .jobmon.ini
# }


#             echo "
#             [client]
#             web_service_fqdn=$TARGET_IP
#             web_service_port=80
#             " > .jobmon.ini
