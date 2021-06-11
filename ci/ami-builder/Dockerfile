FROM debian:bullseye

RUN apt-get update \
 && apt-get dist-upgrade --yes \
 && apt-get install --yes --no-install-recommends \
  packer \
 && apt-get clean

COPY ./assets/root /
