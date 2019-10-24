#!/bin/bash

set -e

if [ "$DOWNLOAD_TALKER" = true ] ; then
  sudo mkdir -p /root/talker
  cd /root/talker
  curl -sfL https://github.com/weka-io/talker/raw/"$TALKER_AGENT_RELEASE"/talker_agent/talker-service > talker-service
fi

curl -sfL https://github.com/weka-io/talker/releases/download/"$TALKER_AGENT_RELEASE"/talker.bin > talker.bin
chmod +x talker.bin

echo "$TALKER_AGENT_RELEASE" > version
