#!/bin/sh

echo "$TALKER_VERSION" > version
echo {\"password\": null, \"port\": 6379, \"host\": \"redis\", \"host_id\": \"$TALKER_HOST_ID\"} > conf.json

# Add redis py dependency
curl -sfL https://github.com/andymccurdy/redis-py/archive/"$TALKER_REDIS_VERSION".tar.gz | tar -xz
mv redis-py-"$TALKER_REDIS_VERSION"/redis .
rm -rf redis-py-"$TALKER_REDIS_VERSION"