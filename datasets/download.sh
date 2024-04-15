#!/bin/bash
set -exo

cd data
if [ ! -f "COOL-1.0.112.jar" ]; then 
    curl -k -o "COOL-1.0.112.jar" -L https://github.com/big-unibo/conversational-olap/releases/download/1.0.112/COOL-all.jar
fi
curl -k -o watering_red_20240125T142437.sql https://big.csr.unibo.it/projects/nosql-datasets/watering_red_20240125T142437.sql
curl -k -o watering-sim_red_20240125T142315.sql https://big.csr.unibo.it/projects/nosql-datasets/watering-sim_red_20240125T142315.sql
curl -k -o cimice_20240125T142421.sql https://big.csr.unibo.it/projects/nosql-datasets/cimice_20240125T142421.sql
cd -

cd libs
if [ ! -f "instantclient-basic-linux.x64-21.1.0.0.0.zip" ]; then
  curl -k -o instantclient-basic-linux.x64-21.1.0.0.0.zip https://big.csr.unibo.it/projects/nosql-datasets/instantclient-basic-linux.x64-21.1.0.0.0.zip
  unzip instantclient-basic-linux.x64-21.1.0.0.0.zip
  chmod -R 777 instantclient_21_1
fi
if [ ! -f "instantclient-basic-windows.x64-21.3.0.0.0.zip" ]; then
  curl -k -o instantclient-basic-windows.x64-21.3.0.0.0.zip https://big.csr.unibo.it/projects/nosql-datasets/instantclient-basic-windows.x64-21.3.0.0.0.zip
  unzip instantclient-basic-windows.x64-21.3.0.0.0.zip
  chmod -R 777 instantclient_21_3
fi
ls -las
cd -