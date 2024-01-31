chmod -R 777 /data
ls -las /data
# expdp sales_v1/oracle@xe directory=oracle_dump dumpfile=sales_v1-2023-06-07.dmp SCHEMAS=sales_v1
# impdp foodmart/oracle@127.0.0.1:1521/xe DIRECTORY=ORACLE_DUMP DUMPFILE=FOODMART.DMP SCHEMAS=foodmart
touch /data/.ready
