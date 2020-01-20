import pymysql
import json


class MySQL:
    #Read setting and make available in settings dict
    with open('conf.json') as data_file:    
        settings = json.load(data_file)

    __HOST = settings["db_host"]
    __USER = settings["db_user"]
    __PASSWORD = settings["db_pass"]
    __DB = settings["db_database"]
    __PORT = 3306

    def __init__(self):
        self.conn = None

    def _connect(self):
        self.conn = pymysql.connect(host=self.__HOST, user=self.__USER, passwd=self.__PASSWORD,
                                    db=self.__DB, port=self.__PORT, charset='utf8', cursorclass=pymysql.cursors.DictCursor)
        self.conn.autocommit(True)
        return self.conn

    def _quit(self):
        self.conn.close()

    def insert_record(self, table_name, inserted_dict, **kwargs):
        insert_val = []

        sql = "INSERT INTO " + table_name + " ("
        for key, value in inserted_dict.items():
            sql += " `"+key+"`, "
        sql = sql[:-2]
        sql += ") VALUES ( "
        for key, value in inserted_dict.items():
            # if key == kwargs['insert_timestamp']:
            if kwargs is not None and 'insert_timestamp' in kwargs and key == kwargs['insert_timestamp']:
                sql += "now(), "
            else:
                sql += "%s, "
                insert_val.append(value)
        sql = sql[:-2]
        sql += " ) "
        # print(sql)

        self._connect()
        
        with self.conn.cursor() as cur:
            try:
                # pass
                cur.execute(sql, insert_val)
            except Exception as e:
                if ('error_msg' in kwargs):
                    print("{}: {}".format(kwargs['error_msg'], e))
                else:
                    print("Error while inserting: {}".format(e))
        
        self._quit()
    
    def execute_many(self, sql, data):
        """ Execute many command for inserting many rows in one query """

        try:
            tablename = sql.split("INSERT INTO")[1].strip().split(" ")[0].strip()
        except Exception:
            pass

        self._connect()

        with self.conn.cursor() as cur:
            try:
                cur.executemany(sql, data)
                if tablename:
                    print('{} Zeilen in {} eingefügt.'.format(cur.rowcount, tablename))
                else:
                    print('{} Zeilen in eingefügt.'.format(cur.rowcount))

            except Exception as e:
                if tablename:
                    print("Error while inserting data to {}: {}".format(tablename, e))
                else:
                    print("Error while inserting data: {}".format(e))
        
        self._quit()



"""

def alter_table(conn, groupresults, alterclause):
    with conn.cursor() as cur:
        sql = 'show tables like \'{}\''.format(groupresults)
        try:
            cur.execute(sql)
        except Exception as e:
            print('Error: Table {} nicht gefunden: {} \n {}'.format(groupresults, e, sql))
            
        table_exists = cur.rowcount
        if table_exists > 0:
            print('Altering table {}: {}'.format(groupresults, alterclause))
            sql = 'ALTER TABLE {} {}'.format(groupresults, alterclause)
            try:
                cur.execute(sql)
            except Exception as e:
                print('Error altering table {}: Alter Clause fehlgeschlagen: {} \n {}'.format(groupresults, e, sql))
            
            success = cur.rowcount
            if success > 0:
                print('Altering table {} successful'.format(groupresults))
            else:
                print('Error occured altering table {}: {}'.format(groupresults, sql))
        else:
            print('Execute altertable: Tabelle {} existiert nicht'.format(groupresults))
            
def delete_table(conn, table, whereclause, i=None):
    def execute_query(conn, table, del_sql):
        with conn.cursor() as cur:
            try:
                cur.execute(del_sql)
                affected_rows = cur.rowcount
                print("{} Records gelöscht".format(affected_rows))
            except Exception as e:
                print('Error: Deleting from table {}: {} \n {}'.format(table, e, del_sql))
        return affected_rows

    with conn.cursor() as cur:
        sql = 'show tables like \'{}\''.format(table)
        try:
            cur.execute(sql)
        except Exception as e:
            print('Error: Table {} nicht gefunden: {} \n {}'.format(table, e, sql))
            
        table_exists = cur.rowcount
        if table_exists > 0:
            print('Deleting from table {}: {}'.format(table, whereclause))
            if i:
                del_sql = "DELETE FROM {0} {1} LIMIT {2}".format(table, whereclause, i)
                affected_rows = 1
                while affected_rows > 0:
                    affected_rows = execute_query(conn, table, del_sql)
            else:
                del_sql = "DELETE FROM {0} {1}".format(table, whereclause)
                execute_query(conn, table, del_sql)
                
def check_existence_of_record(conn, table, col, value):
    sql = "select count(*) from {0} where {1}='{2}'".format(table, col, value)
    with conn.cursor() as cur:
        try:
            cur.execute(sql)
            result_dict = cur.fetchone()
            existing = result_dict['count(*)']
        except Exception as e:
            print("Error checking existence of record: {}".format(e))
    return existing

"""