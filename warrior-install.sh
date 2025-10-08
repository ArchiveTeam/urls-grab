#!/bin/bash

if ! dpkg-query -Wf'${Status}' ghostscript 2>/dev/null | grep -q '^i'
then
  echo "Installing ghostscript..."
  sudo DEBIAN_FRONTEND=noninteractive DEBIAN_PRIORITY=critical apt-get -qqy --no-install-recommends -o Dpkg::Options::=--force-confdef -o Dpkg::Options::=--force-confold -o Dpkg::Options::=--force-unsafe-io update
  sudo DEBIAN_FRONTEND=noninteractive DEBIAN_PRIORITY=critical apt-get -qqy --no-install-recommends -o Dpkg::Options::=--force-confdef -o Dpkg::Options::=--force-confold -o Dpkg::Options::=--force-unsafe-io install ghostscript || exit 1
  sudo rm -rf /var/lib/apt/lists/*
fi

if ! python3 -c "import validators" 2>/dev/null
then
  echo "Installing Python3 validators..."
  sudo python3 -m pip install --no-cache-dir validators || exit 1
fi

exit 0

