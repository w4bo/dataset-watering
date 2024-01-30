#!/bin/bash
set -exo
rm -f resources/.ready
docker compose up --build -d