# watering-dataset

To create a dump of a database

    pg_dump -h your_host -d your_database -U your_username -W --rows=100 -F c -f /path/to/dumpfile.backup

