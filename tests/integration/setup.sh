export TALKER_AGENT_VERSION=$(cat VERSION)
cd tests/integration
export TALKER_HOST_ID=MyHostId
docker-compose up --no-start
docker-compose start
docker-compose ps
cd ../../
