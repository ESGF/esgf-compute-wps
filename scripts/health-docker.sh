#! /bin/bash

recipient=boutte3@llnl.gov
tmp=/tmp/docker-report.txt

echo "Unhealthy containers" > $tmp

echo "" >> $tmp

/usr/bin/docker ps --format "table {{.Image}}\t{{.CreatedAt}}\t{{.RunningFor}}\t{{.Status}}\t{{.Names}}" | grep -E "docker_" | grep -E "\(unhealthy" >> $tmp

echo "" >> $tmp

echo "Unchecked containers" >> $tmp

echo "" >> $tmp

/usr/bin/docker ps --format "table {{.Image}}\t{{.CreatedAt}}\t{{.RunningFor}}\t{{.Status}}\t{{.Names}}" | grep -E "docker_" | grep -E -v "(un)?healthy" >> $tmp

echo "" >> $tmp

echo "Healthy containers" >> $tmp

echo "" >> $tmp

/usr/bin/docker ps --format "table {{.Image}}\t{{.CreatedAt}}\t{{.RunningFor}}\t{{.Status}}\t{{.Names}}" | grep -E "docker_" | grep -E "\(healthy" >> $tmp

echo "" >> $tmp

type mail > /dev/null

if [[ $? -eq 0 ]]
then
  echo "Sending report, recipients \"${recipient}\""

  mail -s "${HOSTNAME} Docker Report" ${recipient} < $tmp
fi
