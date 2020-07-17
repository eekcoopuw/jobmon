#! /usr/bin/env bash

echo "Running inside /app/prestart.sh"
echo "migrating nginx config from /appnginx_app.conf to /etc/nginx/conf.d/nginx.conf"
cp ./nginx_app.conf /etc/nginx/conf.d/nginx.conf
