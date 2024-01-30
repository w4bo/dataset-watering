#!/bin/bash
set -exo
export $(grep -v '^#' .env | xargs)
if [ -f venv/bin/activate ]; then
    source venv/bin/activate
elif  [ -f venv/Scripts/activate ]; then
    source venv/Scripts/activate
fi

for FILE in *.ipynb; do
    echo "$FILE"
    jupyter nbconvert --to notebook --inplace --execute "$FILE"
done