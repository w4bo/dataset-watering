#!/bin/bash
set -exo
chmod 755 dump-clean-ora.sh dump-clean-pg.sh
docker cp dump-clean-ora.sh oracle:/
docker exec oracle /dump-clean-ora.sh
docker cp dump-clean-pg.sh postgis:/
docker exec postgis /dump-clean-pg.sh