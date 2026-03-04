#!/usr/bin/env bash

if [ ! -d .state/pg ]; then
    initdb -D .state/pg
fi

postgres \
    -c default_transaction_isolation=serializable \
    -c unix_socket_directories='' \
    -D .state/pg
