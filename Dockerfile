FROM ubuntu:22.04

ENV DEBIAN_FRONTEND noninteractive

# Install packages
RUN apt-get update && \
  apt-get -y --no-install-recommends install \
    build-essential \
    nginx && \
  apt-get clean && \
  rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*


WORKDIR /

COPY ./storage /storage
COPY ./start /start
COPY ./jakaja /jakaja

RUN chmod +x /start
RUN chmod +x /storage

CMD ./start
