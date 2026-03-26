#!/bin/bash

# This scripts runs hidra under wine and expect to be called by hidra.sh

export WINEPREFIX=/home/hidra/.wine

wine /home/hidra/.wine/drive_c/hidra/get_receiver_status.exe "$@" &

hidra_pid=$!

trap "kill -SIGINT $hidra_pid" SIGINT
# Translate SIGTERM to SIGINT because only the latter works under Windows and
# therefore under wine
trap "kill -SIGINT $hidra_pid" SIGTERM

wait $hidra_pid
