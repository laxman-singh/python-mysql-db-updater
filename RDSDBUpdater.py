#!/usr/bin/env python

''' A python tool to sync RDS Mysql server with a locally running raspberry-pi device with mysql database. '''
''' Author  : Laxman Singh ~ laxman.1390@gmail.com
    Since   : 23 Sept, 2017 12:00:00 AM IST '''


import mysql.connector
from mysql.connector import errorcode
import time

try:
    print("Connecting to RDS server at ::: {}".format(time.ctime()))
    cnx_rds = mysql.connector.connect(user='user',
                                password='passwd',
                                database='Test',
                                host='dev.xyz.us-east-2.rds.amazonaws.com',
                                port='3306',)

    print("Connected to RDS Mysql instance.")
    # connection is success to RDS Mysql Server. lets find out the unsynched records
    print("Connecting to loal mysql at raspberry-pi at ::: ".format(time.ctime()))
    cnx_local = mysql.connector.connect(user='root',
                                database='Test',
                                host='localhost',
                                port='3306',)
    print("Connected to local raspberry-pi Mysql instance.")
    cursor_local = cnx_local.cursor(buffered=True)
    query_local = ("SELECT name, email, phone from replication_tbl where rds_synched = 0")
    cursor_local.execute(query_local)

    # now get the RDS mysql cursor_local
    cursor_rds = cnx_rds.cursor()

    for (name, email, phone) in cursor_local:
        print("{}, {}, {}".format(name, email, phone))
        print("Inserting data to RDS mysql instance")
        insert_query_rds = ("INSERT INTO replication_tbl "
                              "(name, email, phone) "
                              "VALUES (%(name)s, %(email)s, %(phone)s)")
        data_insert_query_rds = {
                      'name': name,
                      'email': email,
                      'phone': phone,
                    }
        cursor_rds.execute(insert_query_rds, data_insert_query_rds)
        print("Data inserted to RDS instance")

        # update raspberry-pi mysql row as sysnched
        print("updating local raspberry-pi mysql row")
        update_query_local = ("UPDATE replication_tbl set rds_synched = 1 where name = (%(name)s)")
        data_update_query_local = {'name': name,}
        cursor_local2 = cnx_local.cursor()
        cursor_local2.execute(update_query_local, data_update_query_local)
        cursor_local2.close()
        print("Successfully completed ")

    # close db connections
    cursor_rds.close()
    cursor_local.close()
    cnx_local.commit()
    cnx_local.close()
    cnx_rds.commit()
except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("Something is wrong with your user name or password")
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("Database does not exist")
    elif err.errno == errorcode.CR_CONNECTION_ERROR:
        print("Connection Error")
    else:
        print(err)
else:
    cnx_rds.close()
