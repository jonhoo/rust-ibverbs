#!/usr/bin/env bash

set -euxETo pipefail

sudo ./scripts/make-rdma-loopback.sh

cargo test
