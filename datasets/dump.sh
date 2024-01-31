#!/bin/bash
set -exo
docker cp dump-clean-ora.sh oracle:/
docker exec -u root -it oracle /dump-clean-ora.sh
docker cp dump-clean-pg.sh postgis:/
docker exec postgis /dump-clean-pg.sh