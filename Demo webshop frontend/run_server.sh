#!/bin/sh
echo "${click_topic}" > /usr/share/nginx/html/click_topic
echo "${offers_topic}" > /usr/share/nginx/html/offers_topic
nginx -g "daemon off;"
