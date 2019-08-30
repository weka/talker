#!/bin/sh

if [ "$DOWNLOAD_TALKER" = true ] ; then
  sudo mkdir -p /root/talker/talker_agent
  cd /root/talker
  curl -sfL https://github.com/weka-io/talker/raw/v1.8.1/talker_agent/talker-service > talker_agent/talker-service
  curl -sfL https://github.com/weka-io/talker/raw/v1.8.1/talker_agent/talker.py > talker_agent/agent.py
  curl -sfL https://github.com/weka-io/talker/raw/v1.8.1/talker_agent/talker.py > talker_agent/config.py
  curl -sfL https://github.com/weka-io/talker/raw/v1.8.1/talker_agent/talker.py > talker_agent/job.py
  curl -sfL https://github.com/weka-io/talker/raw/v1.8.1/talker_agent/talker.py > talker_agent/talker.py
  curl -sfL https://github.com/weka-io/talker/raw/v1.8.1/talker_agent/utils.py > talker_agent/utils.py
  chmod +x talker.py
fi

echo "$TALKER_AGENT_VERSION" > version

# Add redis py dependency
TALKER_REDIS_VERSION=2.10.6
curl -sfL https://github.com/andymccurdy/redis-py/archive/"$TALKER_REDIS_VERSION".tar.gz | tar -xz
mv redis-py-"$TALKER_REDIS_VERSION"/redis .
rm -rf redis-py-"$TALKER_REDIS_VERSION"
