#!/bin/bash
set +e
# set +x

# Replace Socket Timeout of Azure Storage Library
sed "s|SOCKET_TIMEOUT = 11|SOCKET_TIMEOUT = (20, 2000)|g" -i /usr/lib/python3/dist-packages/azure/storage/_constants.py

echo "Preparing Backy2..."
if [ -f /var/lib/backy2/backy.sqlite ]; then
    echo "Initializing Backy DB..."
    backy2 initdb
fi

cat /backy.cfg.template | envsubst > /etc/backy.cfg
cat /etc/backy.cfg

#redirect backy2 logs to stdout and remove internal log file to avoid increasing endlessly
tail -f /var/log/backy.log&
while true; do if [ -f /var/log/backy.log ]; then rm /var/log/backy.log; fi; sleep 86400; done &

tail -f /dev/null