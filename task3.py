import psycopg2
import pandas as pd
import csv
from config import config
import psycopg2.extras

import io
from io import StringIO
import string

df=pd.read_csv('sample_sheet.csv')
#print(df.dtypes)
connection=None
params=config()
connection=psycopg2.connect(**params)
cur=connection.cursor()
#cur.execute("drop table chunk;")
#cur.execute("create table chunk(IMO int,SEQUENCE int,EFFECTIVE_DATE int,ORGANIZATION_NAME varchar(50),ORGANIZATION_CODE varchar(50), ORGANIZATION_STATUS float,VALID_FROM_DATETIME time(7),VALID_TO_DATETIME time(7),VALID_FROM_DATE date,VALID_TO_DATE date,DeathDate date);")
print("table is")
#cur.execute("select * from chunkfile;"))
#print(cur.fetchall())
chunksize=10000
i=0
df_largest_banks=pd.DataFrame()
for chunk in pd.read_csv('sample_sheet.csv',chunksize=chunksize):
    largest_bank=chunk[chunk['IMO']>1000]
    df_largest_banks=pd.concat([df_largest_banks,largest_bank])
    #print(df_largest_banks)
    output = io.StringIO() # For Python3 use StringIO
    largest_bank.to_csv(output, sep='\t', header=True, index=False)
    output.seek(0) # Required for rewinding the String object
    copy_query = "COPY chunk FROM STDOUT csv DELIMITER '\t' NULL ''  ESCAPE '\\' HEADER "  
    cur.copy_expert(copy_query, output)
    connection.commit()
    i=i+1
    
    #cur.execute("select * from chunk;")
    print(i)
    print()
    print()
    print()
    print()
print(df_largest_banks.head)
        
cur.close()
   
    











'''

df=pd.read_csv('sample_sheet.csv')
#print(df.dtypes)
#to read nrows
#print(pd.read_csv('sample_sheet.csv',nrows=5)) #or print(df.head(5))
#print(df.shape) #to find no of rows and col
#print(df.memory_usage(True)) # to check memory usage
connection=None
params=config()
connection=psycopg2.connect(**params)
cur=connection.cursor()
#cur.execute("drop table chunkfile;")
#cur.execute("create table chunkfile(IMO int,SEQUENCE int,EFFECTIVE_DATE int,ORGANIZATION_NAME varchar(50),ORGANIZATION_CODE varchar(50), ORGANIZATION_STATUS float,VALID_FROM_DATETIME time(7),VALID_TO_DATETIME time(7),VALID_FROM_DATE date,VALID_TO_DATE date,DeathDate date);")
print("table is")
#cur.execute("select * from chunkfile;"))
#print(cur.fetchall())
chunksize=10
i=0
df_largest_banks=pd.DataFrame()
for chunk in pd.read_csv('sample_sheet.csv',chunksize=chunksize):
    largest_bank=chunk[chunk['IMO']>1000]
    df_largest_banks=pd.concat([df_largest_banks,largest_bank])
    #print(df_largest_banks)
    if len(largest_bank) > 0:
        df_columns = list(largest_bank)
        # create (col1,col2,...)
        columns = ",".join(df_columns)
       
       
        # create VALUES('%s', '%s",...) one '%s' per column
        #values = "VALUES({})".format(",".join(["%s" for _ in df_columns])) 
        values='VALUES("%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s")'
        
        #print("values are")
        #print(values)
        
            #create INSERT INTO table (columns) VALUES('%s',...)
        insert_stmt = "INSERT INTO {} ({}) {}".format(chunk,columns,values)
        
        #print("insertstatements are")
        #print(insert_stmt)
        #print("chunk table")
        #psycopg2.extras.execute_batch(cur, insert_stmt, largest_bank.values)
        
        connection.commit()
        #cur.execute("select * from chunk;")
        #print(cur.fetchall())
        cur.close()
        break
        
print("OVER")
#print(df.dtypes)
#print(df_largest_banks.dtypes)
#print(df_largest_banks.shape)
'''

























'''
def connect():
    connection=None
    params=config()
    print("connecting")
    connection=psycopg2.connect(**params)
    cur=connection.cursor()

    
    #print("postgresqlversion")
    #cur.execute('select version()')
    #cur.execute("drop table oldfile")
   
    
    
    #cur.execute("COPY chunk  FROM '\sample_sheet.csv'  WITH( FORMAT CSV, HEADER, DELIMITER ',' ) WHERE 'IMO'= '5321320';")
    
    
    
    #cur.execute("insert into oldfile values (1,'siri') ;")
    #cur.execute("alter table oldfile drop column checksum;")

    #cur.execute("select * from oldfile;")
    #print(cur.fetchall())
    #to check all tables from a database
    #cur.execute("select relname from pg_class where relkind='r' and relname !~ '^(pg_|sql_)';")
    #tuples=cur.fetchall()
    #for i in tuples:
     #   print(str(list(i)))
    connection.commit()

    #Sdb_version=cur.fetchone()
    #print(db_version)





    
    cur.close()
    if connection is not None:
        connection.close()
        print("databse closed")
if __name__=="__main__":
    connect()
'''
  