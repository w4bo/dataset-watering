from shared import *

with open('dump.sh', 'w') as f:
    f.write('#!/bin/bash\nset -exo\n')
    f.write('pg_dump -h {} -d {} -U {} -O -x -t synthetic_data -t synthetic_field -t synthetic_scenario -t synthetic_field_scenario -t synthetic_scenario_arpae_data -t synthetic_scenario_water_data -f /watering-sim_$(date +%Y%m%dT%H%M%S).sql\n'.format(config['IN_HOST'], config['IN_DB'], config['IN_USER']))
    f.write('pg_dump -h {} -d {} -U {} -O -x -t data_interpolated -t view_data_original -f /watering_$(date +%Y%m%dT%H%M%S).sql\n'.format(config['IN2_HOST'], config['IN2_DB'], config['IN2_USER']))
    f.write('pg_dump -h {} -d {} -U {} -O -x -t test_dim_trap -t test_bridge_trap_acque_interne -t test_bridge_trap_case -t test_bridge_trap_uso_suolo -t test_fact_passive_monitoring_normalized -t test_dim_acque_interne -t test_dim_case -t test_dim_uso_suolo -f /cimice_$(date +%Y%m%dT%H%M%S).sql'.format(config['IN3_HOST'], config['IN3_DB'], config['IN3_USER']))