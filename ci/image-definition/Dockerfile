# This target constructs necessary deb files
FROM debian:bullseye as package_build

# Add deb-src dependencies to sources.list
# This is handy to make debian sort out most of the build dependencies for us with the build-dep command to apt-get.
RUN cp /etc/apt/sources.list /tmp/ \
 && sed -i 's|^deb |deb-src |' /tmp/sources.list \
 && cat /tmp/sources.list >> /etc/apt/sources.list \
 && rm /tmp/sources.list

# The linux kernel in prepackaged AMIs generally does not come with the rdma_rxe kernel module available.  Sadly, we
# need to build our own for testing.
RUN apt-get update \
 && apt-get dist-upgrade --yes \
 # Install build dependencies for the linux kernel
 && apt-get build-dep --yes --no-install-recommends linux-image-amd64 \
 # Unfortunately, not everything we need comes with the build-dep command
 && apt-get install --yes --no-install-recommends \
  bc \
  bison \
  ca-certificates      `#needed for kernel fetch` \
  dwarves \
  flex \
  libelf-dev \
  libncurses-dev       `#needed for "make menuconfig"` \
  libssl-dev \
  lsb-release          `#needed for "make" command` \
  pkg-config \
  wget                 `#needed for kernel source fetch` \
  zstd                 `#needed to compress the linux kernel in my prefered way`

# make a place to store all the debs we about to build.
RUN mkdir --parent /tmp/debs

# Build the current linux kernel (current as of 2021-06-09)
# Copy in rust_ibverbs testing kernel configuration.
# This kernel is specifically built to be very close to the stock AWS debian-buster kernel save that I added some
# networking functionality and enabled infiniband/RDMA and switchdev support.
# Specifially, I added support for Amazon's elastic fabric adapter and all supported Mellanox cards and SoftRoCE.
# Also, I ran make-oldconfig on this to bring all the kernel build parameters up to date.
# If we bump the kernel version in the future we will need to run make-oldconfig again to make sure we have a valid
# kernel config.
ENV KERNEL_VERSION="5.14.9"
RUN mkdir --parent /tmp/linux-kernel-build \
 && wget --quiet --output-document=- "https://cdn.kernel.org/pub/linux/kernel/v5.x/linux-${KERNEL_VERSION}.tar.xz" \
  | tar --extract --xz --directory=/tmp/linux-kernel-build/
COPY ./assets/kernel.config /tmp/linux-kernel-build/linux-"${KERNEL_VERSION}"/.config
#RUN false
#RUN cd /tmp/linux-kernel-build/linux-"${KERNEL_VERSION}" \
# && make ARCH=x86_64 --jobs="$(( $(nproc) + 1 ))" deb-pkg
#
## Toss those debs into another directory so we can bring them all into the next build stage easily
#RUN mv /tmp/linux-kernel-build/*.deb /tmp/debs
#
## We need to build a shiny new iproute2 to gain access to the relevant rdma tool to add an rdma rxe device inside the
## VM (the one which ships with the stock debian AMI is missing the necessary features).
## iproute2 is versioned with the linux kernel but it generally does not have patch releases.
## The version debian ships with is usually very old (typical of Debian) and not at all aligned with the kernel we are
## using here.
## Thus we grab the build recipe from the wonderful debian snapshots, grab the iproute2 code from kernel.org, and put
## them together (with minor modifications to the build recipe to adapt it to the newer version).
## We use debian snapshots instead of the official mirror so that our build doesnt break if debian increments the minor
## version.
## We should be able to adjust the snapshot date every now and then to keep things current.
#ENV DEBIAN_SNAPSHOT_DATE="20210609T203216Z"
#ENV DEBIAN_SNAPSHOT_URL="https://snapshot.debian.org/archive/debian/${DEBIAN_SNAPSHOT_DATE}/pool/main"
#ENV IPROUTE2_VERSION=5.12.0
## This is the latest deb config version in debian at the moment.
#ENV IPROUTE2_DEB_CONFIG_FILE="iproute2_5.10.0-4.debian.tar.xz"
#
## Install build dependencies for iproute2
#RUN apt-get update \
# && apt-get build-dep --yes --no-install-recommends iproute2
#
#RUN mkdir /tmp/iproute2-build \
# && cd /tmp/iproute2-build \
# && wget --quiet --output-document=- \
#      "https://git.kernel.org/pub/scm/network/iproute2/iproute2.git/snapshot/iproute2-${IPROUTE2_VERSION}.tar.gz" \
#  | tar --extract --gz --directory=/tmp/iproute2-build/ \
# && cd /tmp/iproute2-build/iproute2-"${IPROUTE2_VERSION}" \
# && wget --quiet --output-document=- \
#      "${DEBIAN_SNAPSHOT_URL}/i/iproute2/${IPROUTE2_DEB_CONFIG_FILE}" \
#  | tar --extract --xz --directory=/tmp/iproute2-build/iproute2-"${IPROUTE2_VERSION}" \
# && cd /tmp/iproute2-build/iproute2-"${IPROUTE2_VERSION}" \
# # zap all the patches debian would normally apply.  Just build clean iproute2
# && rm debian/patches/*.patch \
# && : > debian/patches/series \
# # we need to add a couple files to the install list which the debian installer is not expecting at the moment
# # to make this package build
# && echo sbin/dcb >> debian/iproute2.install \
# && echo sbin/vdpa >> debian/iproute2.install \
# # I use the --build=any,all flag here to make dpkg-buildpackage chill out.  This build isn't going in a published repo
# # so all those checks are not necessary.
# && DEB_BUILD_OPTIONS="parallel=$(( $(nproc) + 1 ))" dpkg-buildpackage -us -uc --build=any,all  \
# && mv /tmp/iproute2-build/*.deb /tmp/debs
#
## Package rmda-core (the one that ships with debian is not the same as the one rust_ibverbs builds against).
#
## Install build dependencies for rdma-core
#RUN apt-get update \
# && apt-get build-dep --yes --no-install-recommends rdma-core \
# && apt-get install --yes --no-install-recommends \
# git `#needed to fetch rdma-core source`
#
#ENV RDMA_CORE_VERSION="34.0"
#RUN mkdir -p /tmp/rdma-core-build \
# && cd /tmp/rdma-core-build \
# && git clone --depth 1 --branch "v${RDMA_CORE_VERSION}" https://github.com/linux-rdma/rdma-core.git \
# && cd rdma-core \
# && tar czvf ../rdma-core_${RDMA_CORE_VERSION}.orig.tar.gz . \
# && DEB_BUILD_OPTIONS="parallel=$(( $(nproc) + 1 ))" dpkg-buildpackage --jobs=auto -us -uc -ui \
# && mv /tmp/rdma-core-build/*.deb /tmp/debs
#
## Don't need the debug symbols for all these libs
#RUN rm /tmp/debs/*dbgsym*.deb /tmp/debs/*-dbg_*.deb
#
## Now that we have built all the deb files we need, cook up a base operating system image with just what we need for
## testing rust_ibverbs.
#FROM debian:bullseye as bootstrap
#
#RUN apt-get update \
# && apt-get dist-upgrade --yes \
# && apt-get install --yes --no-install-recommends \
#   debootstrap
#
#RUN cd / \
# && debootstrap \
#  --variant=minbase \
#  testing `#bullseye is currently in testing for a bit longer.  Move to stable when it is officially out.` \
#  rootfs
#
#FROM scratch as provision
#COPY --from=bootstrap /rootfs /
## Fill out the rest of the base operating system
#RUN apt-get update \
# && apt-get dist-upgrade --yes \
# && apt-get install --yes --no-install-recommends \
#  apt-transport-https    `#needed for docker` \
#  binutils               `#needed to generate unified kernel image with objcopy` \
#  ca-certificates        `#needed for docker` \
#  cloud-init             `#needed to boot in the cloud` \
#  curl                   `#needed for docker` \
#  dracut                 `#needed to generate initramfs` \
#  gnupg                  `#needed for docker` \
#  gnupg                  `#needed to process docker gpg public key when we install docker` \
#  isc-dhcp-client        `#needed to receive network address via dhcp` \
#  kmod                   `#needed to modprobe kernel modules` \
#  libbpf0                `#needed for iproute2` \
#  libbsd0                `#needed for iproute2` \
#  libcap2-bin            `#needed for iproute2` \
#  libmnl0                `#needed for iproute2` \
#  libnl-3-dev            `#needed for various rdma-core/iproute2 packages` \
#  libnl-route-3-dev      `#needed for various rdma-core/iproute2 packages` \
#  libpython3.9-dev       `#needed for pyverbs` \
#  libxtables12           `#needed for iproute2` \
#  linux-base             `#base linux system` \
#  lsb-release            `#needed for docker` \
#  mount                  `#needed to mount filesystems` \
#  netbase                `#base networking` \
#  openssh-client         `#needed to control docker and communicate with depoyed system` \
#  openssh-server         `#needed to control docker and communicate with depoyed system` \
#  perl                   `#needed for many things :(` \
#  python3                `#needed for several rdma-core packages` \
#  sudo                   `#needed for AWS cloud-init debian user to be functional` \
#  systemd                `#init system for debian` \
#  systemd-sysv           `#init system for debian` \
#  udev                   `#needed to boot` \
#  zstd                   `#needed to compress initramfs`
#
## install most recent docker-ce (the one Debian ships whith is always ancient)
## Docker is not strictly required but it sure does make it easier to update the deployment environment of the CI system.
#RUN curl --fail --silent --show-error --location https://download.docker.com/linux/debian/gpg \
#  | gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg \
# && printf "deb [arch=amd64 signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/debian bullseye stable\n" \
#  | tee /etc/apt/sources.list.d/docker.list > /dev/null \
# && apt-get update \
# && apt-get install --yes --no-install-recommends \
#  containerd.io \
#  docker-ce \
#  docker-ce-cli
#
#COPY --from=package_build /tmp/debs/ /tmp/debs/
#RUN dpkg --install /tmp/debs/*.deb
#
## Set a default root password for local testing
#RUN printf "%s\n" "root:rust_ibverbs" | chpasswd
#
## Enable needed services
#RUN systemctl enable cloud-init \
# && systemctl enable systemd-networkd
#
#COPY assets/provision/root/ /
#
#ENV KERNEL_VERSION="5.12.9"
## Generate a general purpose initramfs which should boot on most x86_64 machines
#RUN dracut \
#  --compress zstd \
#  --force \
#  --kver "${KERNEL_VERSION}" \
#  --no-hostonly \
#  --no-machineid \
#  --persistent-policy=label \
#  --reproducible \
#  "/boot/initrd.img-${KERNEL_VERSION}"
#
## Generate the unified kernel's command line
#RUN printf "root=LABEL=ROOT rw console=tty0 earlyprintk=tty0 console=ttyS0,115200n8 earlyprintk=ttyS0,115200n8 intel_iommu=on iommu=pt\n" > /kernel.cmdline
#
## Produce a unified linux kernel + initramfs + command line (this should make it trivial to boot on a properly
## configured VM, most physical gear, and in AWS assuming the AMI is configured with EFI over legacy BIOS)
#RUN mkdir --parent /boot/EFI/BOOT/ \
# && objcopy \
#  --add-section .osrel="/etc/os-release"                     --change-section-vma .osrel=0x20000 \
#  --add-section .cmdline="/kernel.cmdline"                   --change-section-vma .cmdline=0x30000 \
#  --add-section .linux="/boot/vmlinuz-${KERNEL_VERSION}"     --change-section-vma .linux=0x40000 \
#  --add-section .initrd="/boot/initrd.img-${KERNEL_VERSION}" --change-section-vma .initrd=0x4000000 \
#  /usr/lib/systemd/boot/efi/linuxx64.efi.stub /boot/EFI/BOOT/BOOTX64.EFI
#
## Clean up for the final image
#RUN rm --force --recursive /tmp/debs \
# && apt-get clean \
# && rm --force --recursive /var/lib/apt/lists/* \
# && rm --force --recursive /var/log/* \
# && rm --force --recursive /var/cache/ldconfig/aux-cache \
# && rm --force /etc/machine-id \
# && rm --force /var/lib/dbus/machine-id
#
## This serves to squash the final image into one major layer
#FROM scratch as image
#COPY --from=provision / /
