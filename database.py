import psycopg2
from configparser import ConfigParser

def firstconfig(filename='conf1.ini', section='postgresql'):
        parser = ConfigParser()
        parser.read(filename)
        db = {}
        if parser.has_section(section):
            params = parser.items(section)
            for param in params:
                db[param[0]] = param[1]
        else:
            raise Exception('Section {0} not found in the {1} file'.format(section, filename))
        return db
    
def config(filename='conf.ini', section='postgresql'):
        parser = ConfigParser()
        parser.read(filename)
        db = {}
        if parser.has_section(section):
            params = parser.items(section)
            for param in params:
                db[param[0]] = param[1]
        else:
            raise Exception('Section {0} not found in the {1} file'.format(section, filename))
        return db

class connection():
    def __init__(self, params = config()):
        super().__init__()
        
    def create_database():
        params = firstconfig()
        conn=None
        try:
            conn = psycopg2.connect(**params)
            conn.autocommit = True
            cursor = conn.cursor()
            sql = "CREATE DATABASE coinmarketcap"
            cursor.execute(sql)
            print("Database created successfully........")
            conn.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            if conn is not None:
                conn.rollback()
        finally:
            if conn is not None:
                conn.close()
        
    def connect():
        params = config()
        conn=None
        try:
            conn= psycopg2.connect(**params)
            print("Connection to database succeeded...")
            conn.close()
        except:
            print("!!!Connection to database unsucceeded !!!")
            conn.rollback()
            
    def create_tables(flag):
        conn = None
        if flag == 0: #creat table for block
            commands = (
            """
            CREATE TABLE spot (
                spot_id SERIAL PRIMARY KEY,
                time_stamp VARCHAR(22) NOT NULL,
                price bigint NOT NULL,
                deep2pos bigint NOT NULL,
                deep2neg bigint NOT NULL,
                volume bigint NOT NULL,
                pervol integer NOT NULL,
                liqu integer NOT NULL
            );
            """)   
        elif flag == 1: #creat table for transaction
            commands = (
            """
            CREATE TABLE perpetual (
                perpetual_id SERIAL PRIMARY KEY,
                time_stamp VARCHAR(22) NOT NULL,
                price bigint NOT NULL,
                indprice bigint NOT NULL,
                basis real NOT NULL,
                volume bigint NOT NULL,
                pervol integer NOT NULL,
                fundrate real NOT NULL,
                openinter bigint NOT NULL
            );
            """)  
        elif flag == 2: #creat table for input output
            commands = (
            """
            CREATE TABLE futures (
                perpetual_id SERIAL PRIMARY KEY,
                time_stamp VARCHAR(22) NOT NULL,
                price bigint NOT NULL,
                indprice bigint NOT NULL,
                basis real NOT NULL,
                volume bigint NOT NULL,
                pervol integer NOT NULL,
                openinter bigint NOT NULL
            );
            """)  
        try:
            params = config()
            conn = psycopg2.connect(**params)
            cur = conn.cursor()
            cur.execute(commands)
            cur.close()
            conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            if conn is not None:
                conn.rollback()
        finally:
            if conn is not None:
                conn.close()
    
    def Droptlb(tname):
        conn = None
        commands=("""DROP TABLE  %s""" % (tname))
        try:
            params = config()
            conn = psycopg2.connect(**params)
            cur = conn.cursor()
            cur.execute(commands)
            cur.close()
            conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            if conn is not None:
                conn.rollback()
        finally:
            if conn is not None:
                conn.close()
                
    def insertTOspot(time_stamp, price, deep2pos, deep2neg, volume, pervol, liqu):
        conn = None
        commands = (
        """
        INSERT INTO spot (time_stamp, price, deep2pos, deep2neg, volume, pervol, liqu) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s);
        """)   
        data = (time_stamp, price, deep2pos, deep2neg, volume, pervol, liqu)
        try:
            params = config()
            conn = psycopg2.connect(**params)
            cur = conn.cursor()
            cur.execute(commands, data)
            cur.close()
            conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            if conn is not None:
                conn.rollback()
        finally:
            if conn is not None:
                conn.close()


    def insertTOperpetual(time_stamp, price, indprice, basis, volume, pervol, fundrate, openinter):
        conn = None
        commands = (
        """
        INSERT INTO perpetual (time_stamp, price, indprice, basis, volume, pervol, fundrate, openinter)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
        """)   
        data = (time_stamp, price, indprice, basis, volume, pervol, fundrate, openinter)
        try:
            params = config()
            conn = psycopg2.connect(**params)
            cur = conn.cursor()
            cur.execute(commands, data)
            cur.close()
            conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            if conn is not None:
                conn.rollback()
        finally:
            if conn is not None:
                conn.close()
                
                
    def insertTOfutures(time_stamp, price, indprice, basis, volume, pervol, openinter):
        conn = None
        commands = (
        """
        INSERT INTO futures (time_stamp, price, indprice, basis, volume, pervol, openinter ) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s);
        """)   
        data = (time_stamp, price, indprice, basis, volume, pervol, openinter)
        try:
            params = config()
            conn = psycopg2.connect(**params)
            cur = conn.cursor()
            cur.execute(commands, data)
            cur.close()
            conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            if conn is not None:
                conn.rollback()
        finally:
            if conn is not None:
                conn.close()
                
    #*****************select         
    def selectfrom(table , time_stamp = None):
        conn = None
        if time_stamp == None :
            commands=("""SELECT * FROM %s """ %(table))
        else:
            commands=("""SELECT * 
                      FROM %s
                      WHERE time_stamp == %s
                      """ %(table,time_stamp))
        try:
            params = config()
            conn = psycopg2.connect(**params)
            cur = conn.cursor()
            cur.execute(commands)
            result = cur.fetchall()
            cur.close()
            conn.commit()
            return(result)
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            if conn is not None:
                conn.rollback()
        finally:
            if conn is not None:
                conn.close()
                