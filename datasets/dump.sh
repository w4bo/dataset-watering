#!/bin/bash
set -xo
chmod 755 dump-clean-ora.sh dump-clean-pg.sh
docker cp dump-clean-ora.sh oracledb:/
docker exec oracledb /dump-clean-ora.sh
docker cp dump-clean-pg.sh postgis:/
docker exec postgis /dump-clean-pg.sh
sudo chmod 755 data/*
