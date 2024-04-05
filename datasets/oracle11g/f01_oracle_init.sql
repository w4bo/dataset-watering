CREATE DIRECTORY ORACLE_DUMP as '/data';

create user measurement identified by oracle;
create user cimice identified by oracle;

grant all privileges to measurement;
grant all privileges to cimice;

grant read, write on directory oracle_dump to system;
grant read, write on directory oracle_dump to measurement;
grant read, write on directory oracle_dump to cimice;