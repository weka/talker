FROM python:2.7-slim-buster

RUN apt-get update -y && \
    apt-get install curl locales python-pip -y && \
    pip install redis==3.3.7 nuitka==0.6.5
RUN echo "LC_ALL=en_US.UTF-8" >> /etc/environment && \
    echo "en_US.UTF-8 UTF-8" >> /etc/locale.gen && \
    echo "LANG=en_US.UTF-8" > /etc/locale.conf && \
    locale-gen en_US.UTF-8

COPY talker-service /etc/init.d/talker
RUN chmod +x /etc/init.d/talker && \
    update-rc.d talker defaults

ARG TALKER_AGENT_VERSION
RUN mkdir /etc/talker && \
    echo "$TALKER_AGENT_VERSION" > /etc/talker/version
COPY config.ini /etc/talker/config.ini

# TODO: build outside and copy binary
COPY talker.py ./
RUN nuitka talker.py --follow-imports -o /usr/local/bin/talker
RUN chmod +x /usr/local/bin/talker

CMD service talker start && tail -f /dev/null
