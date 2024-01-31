#!/bin/bash
set -exo
export $(grep -v '^#' .env | xargs)
if [ -f venv/bin/activate ]; then
    source venv/bin/activate
elif  [ -f venv/Scripts/activate ]; then
    source venv/Scripts/activate
fi

until [ -f data/.ready ]
do
    docker logs oracledb | tail -n 10
    sleep 10
done
echo "All databases have been imported!"

for FILE in *.ipynb; do
    echo "$FILE"
    jupyter nbconvert --to notebook --inplace --execute "$FILE"
done