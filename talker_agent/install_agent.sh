#!/bin/bash

set -e

if [ "$DOWNLOAD_TALKER" = true ] ; then
  # talker agent version can be:
  #       tag            - for example: 1.8.2
  #       tag-commit_id  - for example: 1.8.2-02ee1aed8275c79a5d0840505caaaef0fa53b26e
  if [[ $TALKER_AGENT_VERSION == *"-"* ]]; then
    IFS='-' read -r -a parts <<< "$TALKER_AGENT_VERSION"
    git_version=${parts[1]}
  else
    git_version=v$TALKER_AGENT_VERSION
  fi

  sudo mkdir -p /root/talker
  cd /root/talker
  curl -fL https://github.com/weka-io/talker/raw/"$git_version"/talker_agent/talker-service > talker-service
  curl -fL https://github.com/weka-io/talker/raw/"$git_version"/talker_agent/talker.py > talker.py
  chmod +x talker.py
fi

echo "$TALKER_AGENT_VERSION" > version

# Add redis py dependency
REDIS_COMMIT_ID=ad84781ea260be0a1ca4bf6768959b50e8835a6b
curl -fL https://github.com/weka-io/redis-py/archive/"$REDIS_COMMIT_ID".tar.gz -o redis-py-"$REDIS_COMMIT_ID".tar.gz
tar -xzf redis-py-"$REDIS_COMMIT_ID".tar.gz
mv redis-py-"$REDIS_COMMIT_ID"/redis .
rm -rf redis-py-"$REDIS_COMMIT_ID" redis-py-"$REDIS_COMMIT_ID".tar.gz
