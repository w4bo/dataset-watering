#!/bin/bash
set -exo
cp .env.example .env

P=$(pwd)
echo $P
sed -i "s+\!HOME\!+${P}+g" .env
