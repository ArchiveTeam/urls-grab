#!/bin/bash

if ! dpkg-query -Wf'${Status}' ghostscript 2>/dev/null | grep -q '^i'
then
  echo "Installing ghostscript..."
  sudo apt-get update
  sudo apt-get install -y --no-install-recommends ghostscript || exit 1
  sudo rm -rf /var/lib/apt/lists/*
fi

if ! python3 -c "import validators" 2>/dev/null
then
  echo "Installing Python3 validators..."
  sudo python3 -m pip install --no-cache-dir validators || exit 1
fi

exit 0

