#*************************************************************************/
#* PHPBB - MYSQL TO POSTGRESQL - MIGRATION SCRIPT			 */
#* ============================================                          */
#*                                                                       */
#* Copyright (c) 2006-2008 by Federico Campoli (neo@thezion.net)         */
#*                                                                       */
#* This program is free software. You can redistribute it and/or modify  */
#* it under the terms of the GNU General Public License as published by  */
#* the Free Software Foundation; either version 2 of the License.        */
#*************************************************************************/
# VERSION 0.8Beta
# copy structure and data from mysql to postgresql. works with phpbb
# script dependencies
# MySQLdb - http://sourceforge.net/projects/mysql-python
# Psycopg2 
# changes from 0.7
# add blob to bytea datatype creation and transfer 


# FUNCTION LIBRARY
def generate_data_type(v_field_type,v_lenght):
	"generate the correct postgres ddl entry for datatype"
	#field int8
	if v_field_type=="mediumint" or v_field_type=="bigint":
		v_field_pg_type="int8"
		return v_field_pg_type
	
	#field int4
	if v_field_type=="tinyint" or v_field_type=="int" or v_field_type=="smallint":
		v_field_pg_type="int4"
		return v_field_pg_type
	
	#field date
	if v_field_type=="datetime":
		v_field_pg_type="date"
		return v_field_pg_type
		
	#field text
	if v_field_type=="enum" or v_field_type=="tinytext" or v_field_type=="longtext" or v_field_type=="mediumtext":
		v_field_pg_type="text"
		return v_field_pg_type
	
	#field blob
	if v_field_type=="longblob" or v_field_type=="mediumblob" or v_field_type=="blob" or v_field_type=="tinyblob":
		v_field_pg_type="bytea"
		return v_field_pg_type
		
	#field float8
	if v_field_type=="double":
		v_field_pg_type="float8"
		return v_field_pg_type
	
	#field float4
	if v_field_type=="float":
		v_field_pg_type="float4"
		return v_field_pg_type
	
	return v_field_type+ " " + v_lenght
	


def escape_char(sql_string):
	"escape ' character for sql string"
	if sql_string!=None:
		new_sql_string=string.replace(sql_string,"\\","")
		new_sql_string=string.replace(new_sql_string,"'","''")
	else:
		new_sql_string=sql_string

	return new_sql_string

def make_bytea(binary_string):
	"generate a binary string escaped for bytea use"
	if binary_string!=None:
		binary_string=psycopg2.Binary(binary_string)
		binary_string=str(binary_string)
	else:
		binary_string=''

	return binary_string

#import objects string and library for mysql and postgresql
import string
import MySQLdb
import psycopg2
import os
import re

#datadictionary for mysql to postgresql translation
dic_datatype={'mediumint':'int8','tinyint':'int2','smallint':'int2','int':'int8','varchar':'varchar','bigint':'int8','text':'text','char':'char','datetime':'date','longtext':'text','tinytext':'text','tinyblob':'bytea','mediumblob':'bytea','longblob':'bytea','blob':'bytea'}
dic_datavalue={'None':'Null','CURRENT_TIMESTAMP':'CURRENT_TIMESTAMP'}
dic_null={'YES':'NULL','':'NOT NULL'}

#variable multi read. 
i_multi_read=10000



#connection opening to mysql and postgresql
#you need to change the next two lines for your connections
mysql_conn = MySQLdb.connect(db='MYSQLDB', host='MYSQLHOST', user='MYSQLUSR', passwd='MYSQLPASS')
pgsql_conn = psycopg2.connect("dbname=PGDATABASE user=PGUSER host=PGHOST password=PGPASS port=PGPORT")


#MIGRATION OF TABLE STRUCTURE
#cursor to mysql 
c_mys = mysql_conn.cursor()

#cursor to postgresql
c_pgs = pgsql_conn.cursor()

#MYSQL SHOW DATA DICTIONARY
c_mys.execute('show tables;')
str_l_tab=c_mys.fetchall()


#DROP AND GENERATE TABLES ON THE POSTGRESQL TARGET DATABASE

for table_name in str_l_tab:
	c_mys.execute('describe '+table_name[0]+';')
	str_d_tab=c_mys.fetchall()
	#drop table
	try:
		v_drop_table_pg='DROP TABLE '+table_name[0]+' ;'
		c_pgs.execute(v_drop_table_pg)
		c_pgs.execute('commit;')
	except:
		print "Error: PG > Drop Table "+table_name[0]+" is not possible"
		
	c_pgs.execute('commit;')
	print "Notice: PG > Create Table: "+table_name[0]
	v_ddl_pg='CREATE TABLE '+table_name[0]+' '
	v_ddl_pg+='('
	#generate DDL for table definition
	for field in str_d_tab:
		t_field_type = string.split(field[1])
		
		#extract data type and attrib for current field
		t_field_len_type = string.split(t_field_type[0],'(')
		str_field_type = t_field_len_type[0]
		try:
			str_field_attr = t_field_type [1]
		except:
			str_field_attr=""
			
		#extract lenght of current field
		try:
			str_field_lengt="("+str(string.split(t_field_len_type [1],')')[0])+")"
		except:	
			str_field_lengt=""		
		
		v_ddl_pg+=str(field[0])+" "+generate_data_type(str(str_field_type),str(str_field_lengt))+", "
	
	v_ddl_pg=v_ddl_pg[0:(len(v_ddl_pg)-2)]
	v_ddl_pg+=');\n\r'
	#create table
	try:
		c_pgs.execute(v_ddl_pg)
		c_pgs.execute('commit;')
	except:
		print v_ddl_pg

#COPY DATA BETWEEN MYSQL AND POSTGRESQL DATABASES 
for table_name in str_l_tab:

		print "Notice: PG > importing data into table: " + table_name[0]
		#extract how many records there are in the table
		c_mys.execute('select count(*) as num_record from '+table_name[0]+';')
		
		#calculate how many iteration needs for data transfer 
		lng_num_record=c_mys.fetchone()
		i_num_read=lng_num_record[0]/i_multi_read
		rng_num_read=range(i_num_read+1)
		v_dml_pg=""
		#copy data between tables
		for rng_item in rng_num_read:
			c_mys.execute('describe '+table_name[0]+';')
			str_field_tab=c_mys.fetchall()
			str_sql='select * from '+table_name[0]+' limit '+str(rng_item*i_multi_read)+', '+str(i_multi_read)+' ;'
			c_mys.execute(str_sql)
			v_dml_pg_def=""
			try:
				
				str_d_tab=c_mys.fetchall()
				for record in str_d_tab:
					
					v_dml_pg='INSERT INTO '+table_name[0]+' VALUES ('
					v_position=0
					for value in record:
						t_field_type=str_field_tab[v_position][1]
						t_field_len_type = string.split(t_field_type,'(')
						t_data_type=dic_datatype[t_field_len_type[0]] 
						
						try:
							v_field_value = dic_datavalue[str(value)]
						except:
							if t_data_type=='bytea':
								v_field_value = ""+make_bytea(str(value))+""
							else:
								v_field_value = "'"+escape_char(str(value))+"'"
						v_dml_pg+=""+v_field_value +","
						v_position=+1
					v_dml_pg=v_dml_pg[0:(len(v_dml_pg)-1)]
					v_dml_pg+=");"
					try:
						c_pgs.execute(v_dml_pg)
						c_pgs.execute('commit;')
					except:
						print str_sql
						print v_dml_pg
						raise ("error on insert")
						
					
					
			
				
				
			except:
				print "Error: PG > Copy data for  table "+table_name[0]+" is not possible"
				
			print str(min(lng_num_record[0],(rng_item+1)*i_multi_read))+" records imported"


#CREATE SEQUENCES CONSTRAINTS AND INDEX
for table_name in str_l_tab:

		print " Alter Table: " + table_name[0]
		v_table_name=table_name[0]
		#determine the table structure
		c_mys.execute('describe '+table_name[0]+';')
		str_desc_tab=c_mys.fetchall()
		v_ddl_pri=""
		for item in str_desc_tab:
			#create sequence for auto_increment data type
			if item[5]=='auto_increment':
				try:
					c_pgs.execute('DROP SEQUENCE '+table_name[0]+'_id_seq CASCADE;')
					c_pgs.execute('commit;')
				except:
					print 'Error: PG > The sequence '+table_name[0]+' is not present'
				try:
					c_pgs.execute("CREATE SEQUENCE "+table_name[0]+"_id_seq INCREMENT 1  MINVALUE 1  MAXVALUE 9223372036854775807  START 1  CACHE 1;")
					c_pgs.execute('commit;')
				except:
					print "Error: PG > Unable to create the sequence "+table_name[0]+"_id_seq"
				v_alter_table="ALTER TABLE "+table_name[0]+" ALTER COLUMN "+item[0]+" SET DEFAULT nextval('"+table_name[0]+"_id_seq'::regclass);"
			
				c_pgs.execute("select max("+item[0]+") from "+table_name[0])
				
				v_max_seq=c_pgs.fetchone()
				#reset sequence current value to max of auto_increment value
				if str(v_max_seq[0])!='None':
					print table_name[0]
					v_max_seq=v_max_seq[0]+1
					c_pgs.execute("ALTER SEQUENCE  "+table_name[0]+"_id_seq RESTART WITH "+str(v_max_seq)+";")
					c_pgs.execute('commit;')
				c_pgs.execute(v_alter_table)
				c_pgs.execute('commit;')
			else:
				
				try:
					v_def_value = dic_datavalue[str(item[4])]
				except:
					v_def_value = str(item[4])
					
					#alter column. set default value
					if v_def_value!="":
						try:
							v_def_value=int(v_def_value)
						except:
							v_def_value="'"+str(v_def_value)+"'"
				
				if v_def_value !="":
					try:
						v_alter_table="ALTER TABLE "+table_name[0]+" ALTER COLUMN "+item[0]+" SET DEFAULT "+ str(v_def_value)
						#print v_alter_table
						c_pgs.execute(v_alter_table)
						c_pgs.execute('commit;')
					except:
						print "Error: PG >  Unable to set default value"
						print v_alter_table
				
			#alter table. create primary keys and index on column
			if item[3]=='PRI':
				if v_ddl_pri=="":
					v_ddl_pri=item[0]
				else:
					v_ddl_pri+=","+item[0]
			elif item[3]=='MUL':
				v_ddl_const="CREATE INDEX  "+v_table_name+"_"+item[0]+"_IDX  ON "+v_table_name+" USING btree ("+item[0]+");"
				try:
					c_pgs.execute(v_ddl_const)
					c_pgs.execute('commit;')
				except:
					print "Error: PG >  Unable to set primary key or index on "+v_table_name
					print v_ddl_const
				
		if v_ddl_pri!="":
			v_ddl_const="ALTER TABLE "+v_table_name+" ADD CONSTRAINT "+v_table_name+"_pkey PRIMARY KEY ("+v_ddl_pri+");"
			try:
				c_pgs.execute(v_ddl_const)
				c_pgs.execute('commit;')
			except:
				print "Error: PG >  Unable to set primary key or index on "+v_table_name
				print v_ddl_const
		


#closing connections
c_pgs.close()
c_mys.close()
