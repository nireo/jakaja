FROM ubuntu:20.04

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

RUN chmod +x /storage
EXPOSE ${PORT}

RUN PORT=${PORT} ./storage ${STORAGE} &
