FROM python:3.7-alpine

#args from docker-compose.yaml
ARG TALKER_AGENT_VERSION
ARG TALKER_HOST_ID

RUN mkdir -p /root/talker
WORKDIR /root/talker

RUN echo {\"password\": null, \"port\": 6379, \"host\": \"redis\", \"host_id\": \"$TALKER_HOST_ID\"} > conf.json
COPY talker.py talker-service install_agent.sh ./

RUN apk add --no-cache curl bash
RUN chmod +x install_agent.sh && ./install_agent.sh

CMD ["python", "talker.py"]
