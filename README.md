Script to copy structure and data from mysql to postgresql without intermediate dump. 

To work properly needs the following database connectors

MySQLdb - http://sourceforge.net/projects/mysql-python
Psycopg2 - http://pypi.python.org/pypi/psycopg2

Changelog 

0.8.1Beta
		added the date key in the dic_datatype dictionary

0.8Beta
		added the binary data type transfer 

0.7Beta
		changed postgresql library to Psycopg2 
		added error handler on create table

06Beta
		fixed wrong display of records imported 
		fixed CURRENT_TIMESTAMP management
		fixed multicolumn primary keys management 
