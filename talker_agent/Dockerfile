FROM python:2.7-slim-buster

#args from docker-compose.yaml
ARG TALKER_AGENT_VERSION

RUN mkdir -p /root/talker
WORKDIR /root/talker

RUN apt-get update -y && apt-get install curl locales -y
RUN echo "LC_ALL=en_US.UTF-8" >> /etc/environment && \
    echo "en_US.UTF-8 UTF-8" >> /etc/locale.gen && \
    echo "LANG=en_US.UTF-8" > /etc/locale.conf && \
    locale-gen en_US.UTF-8

COPY install_agent.sh ./
RUN chmod +x install_agent.sh && ./install_agent.sh

COPY talker-service /etc/init.d/talker
RUN chmod +x /etc/init.d/talker && \
    ln -s /usr/local/bin/python /usr/bin/python && \
    update-rc.d talker defaults

COPY talker.py config.ini ./
RUN chmod +x talker.py

CMD service talker start && tail -f /dev/null
