FROM python:3.7-alpine

#args from docker-compose.yaml
ARG TALKER_AGENT_VERSION
ARG TALKER_HOST_ID

RUN mkdir -p /root/talker/talker_agent
WORKDIR /root/talker

RUN echo {\"password\": null, \"port\": 6379, \"host\": \"redis\", \"host_id\": \"$TALKER_HOST_ID\"} > conf.json
COPY __init__.py agent.py config.py job.py talker.py utils.py install_agent.sh talker_agent/

RUN apk add --no-cache curl bash
RUN chmod +x talker_agent/install_agent.sh && talker_agent/install_agent.sh

ENV PYTHONPATH=.

CMD ["python", "talker_agent/talker.py"]
