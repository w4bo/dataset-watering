from shared import config

with open('dump-orig.sh', 'w') as f:
    f.write('#!/bin/bash\nset -exo\n')
    f.write('T=$(date +%Y%m%dT%H%M%S)\n')
    f.write('pg_dump -h {} -d {} -U {} -O -x -t synthetic_data -t synthetic_field -t synthetic_scenario -t synthetic_field_scenario -t synthetic_scenario_arpae_data -t synthetic_scenario_water_data -f /data/watering-sim_$(date +%Y%m%dT%H%M%S).sql\n'.format(config['IN_HOST'], config['IN_DB'], config['IN_USER']))
    f.write('pg_dump -h {} -d {} -U {} -O -x -t data_interpolated -t view_data_original -f /data/watering_$(date +%Y%m%dT%H%M%S).sql\n'.format(config['IN2_HOST'], config['IN2_DB'], config['IN2_USER']))
    f.write('pg_dump -h {} -d {} -U {} -O -x -t test_dim_trap -t test_bridge_trap_acque_interne -t test_bridge_trap_case -t test_bridge_trap_uso_suolo -t test_fact_passive_monitoring_normalized -t test_dim_acque_interne -t test_dim_case -t test_dim_uso_suolo -f /data/cimice_$(date +%Y%m%dT%H%M%S).sql'.format(config['IN3_HOST'], config['IN3_DB'], config['IN3_USER']))

with open('dump-clean-pg.sh', 'w') as f:
    f.write('#!/bin/bash\nset -exo\n')
    f.write('T=$(date +%Y%m%dT%H%M%S)\n')
    f.write('export PGPASSWORD={}\n'.format(config['OUT_PWD']))
    f.write('pg_dump -h {} -d {} -U {} -O -x -t dt_agent -t dt_field -t dt_time -t ft_measurement -f /data/measurement-dfm_$T.sql\n'.format(config['OUT_HOST'], config['OUT_DB'], config['OUT_USER']))
    f.write('pg_dump -h {} -d {} -U {} -O -x -t cimice_ft_captures -t cimice_dt_trap -t cimice_dt_time -f /data/cimice-dfm_$T.sql\n'.format(config['OUT_HOST'], config['OUT_DB'], config['OUT_USER']))
    f.write('ls -las /data | grep *dfm*sql')
        
with open('dump-clean-ora.sh', 'w') as f:
    f.write('#!/bin/bash\nset -exo\n')
    f.write('T=$(date +%Y%m%dT%H%M%S)\nexport ORACLE_HOME=/u01/app/oracle/product/11.2.0/xe\nexport PATH=$PATH:$ORACLE_HOME/bin\n')
    f.write('expdp {user}/{pwd}@{host}:{port}/{db} directory=oracle_dump dumpfile={user}-dfm_$T.dmp SCHEMAS={user}\n'.format(user=config['OUT_ORA1_USER'], host=config['OUT_ORA1_HOST'], port=config['OUT_ORA1_PORT'], pwd=config['OUT_ORA1_PWD'], db=config['OUT_ORA1_DB']))
    f.write('expdp {user}/{pwd}@{host}:{port}/{db} directory=oracle_dump dumpfile={user}-dfm_$T.dmp SCHEMAS={user}\n'.format(user=config['OUT_ORA2_USER'], host=config['OUT_ORA2_HOST'], port=config['OUT_ORA1_PORT'], pwd=config['OUT_ORA2_PWD'], db=config['OUT_ORA2_DB']))
    f.write('ls -las /data | grep *dfm*dmp')
