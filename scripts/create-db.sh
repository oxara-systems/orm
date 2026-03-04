#!/usr/bin/env bash

psql -h localhost -p 5432 -d postgres -t \
-c "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = 'orm' AND pid <> pg_backend_pid()" \
-c "DROP DATABASE IF EXISTS orm" \
-c "CREATE DATABASE orm"

psql -h localhost -p 5432 -d orm \
-c "CREATE TABLE users (id INTEGER PRIMARY KEY, age INTEGER NOT NULL, name TEXT NOT NULL)"
