#!/bin/bash
set -exo
chmod -R 777 /data
# expdp foodmart/oracle@xe directory=oracle_dump dumpfile=foodmart.dmp SCHEMAS=foodmart
# impdp foodmart/oracle@127.0.0.1:1521/xe DIRECTORY=ORACLE_DUMP DUMPFILE=FOODMART.DMP SCHEMAS=foodmart
touch /data/.ready