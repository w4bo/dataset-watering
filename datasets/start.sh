#!/bin/bash
set -exo
rm -f resources/.ready || true
docker compose up --build -d