FROM debian:bullseye

# Install tools necessary to assemble a physical / virtual disc image
RUN apt-get update \
 && apt-get dist-upgrade --yes \
 && apt-get install --yes --no-install-recommends \
  apt-transport-https `#needed for docker` \
  ca-certificates     `#needed for docker` \
  curl                `#needed for docker` \
  dosfstools          `#needed to make EFI disc partition` \
  gnupg               `#needed for docker` \
  lsb-release         `#needed for docker` \
  parted              `#needed to partition loopback disc image` \
  zstd                `#needed to de/compress generated image` \
  udev                `#needed to silence parted chatter` \
 && apt-get clean

# install most recent docker-ce (the one Debian ships whith is always ancient)
RUN curl --fail --silent --show-error --location https://download.docker.com/linux/debian/gpg \
  | gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg \
 && \
 { \
    printf "deb [arch=amd64 signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] "; \
    printf "https://download.docker.com/linux/debian bullseye stable\n"; \
 } | tee /etc/apt/sources.list.d/docker.list > /dev/null \
 && apt-get update \
 && apt-get install --yes --no-install-recommends \
  containerd.io \
  docker-ce \
  docker-ce-cli

COPY ./assets/generate-image.sh /

CMD ["/generate-image.sh"]
