# Watering dataset

To create a dump of a database

    pg_dump -h 127.0.0.1 -d db -U root -t synthetic_data -t synthetic_field -t synthetic_scenario -t synthetic_field_scenario -t synthetic_scenario_arpae_data -t synthetic_scenario_water_data -F c -f /dump-mf.backup

To restore it

    pg_restore -h 127.0.0.1 -d db -U root -F c /data/dump-20240119.backup

