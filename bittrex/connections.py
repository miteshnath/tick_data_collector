import MySQLdb
import MySQLdb.cursors
import redis


class MySqlConnection:
    def __init__(self, host=None, port=None, database=None, user=None, password=None):
        self.host = host
        self.port = port
        self.port = int(self.port)
        self.database = database
        self.user = user
        self.password = password

    def connect(self):
        try:
            connection = MySQLdb.connect(user=self.user,
                                         passwd=self.password,
                                         host=self.host,
                                         port=self.port,
                                         db=self.database,
                                         cursorclass=MySQLdb.cursors.DictCursor
                                         )
        except Exception as exp:
            #log.debug("exception while connecting to mysqldb : {0} \n".format(exp))
            print "exception while connecting to mysqldb : {0} \n".format(exp)
        return connection

    def extract(self, query=None):
        conn = self.connect()
        cursor = conn.cursor()
        # log.debug("extracting => query - {0} \n".format(query))
        print "extracting => query - {0} \n".format(query)
        try:
            cursor.execute(query)
            rows = cursor.fetchall()
        except Exception as exp:
            # log.debug("exception : {0} while extacting => query on mysqldb : {1} \n".format(exp, query))
            print "exception : {0} while extacting => query on mysqldb : {1} \n".format(exp, query)
        conn.close()
        return rows

    def execute(self, query=None):
        conn = self.connect()
        cursor = conn.cursor()
        # log.debug("executing sql query - {0} \n".format(query))
        print "executing sql query - {0} \n".format(query)
        try:
            cursor.execute(query)
            conn.commit()
        except Exception as exp:
            # log.debug("exception : {0} while executing => query on mysqldb : {1} \n".format(exp, query))
            print "exception : {0} while executing => query on mysqldb : {1} \n".format(exp, query)
        conn.close()


class RedisConnection:
    def __init__(self, host=None, port=None, database=0):
        global redis_connection_pool
        self.host = host
        self.port = port
        self.port = int(self.port)
        self.database = database
        redis_connection_pool = redis.ConnectionPool(host=self.host, port=self.port, db=self.database)

    def connect(self):
        try:
            connection = redis.Redis(connection_pool=redis_connection_pool)
        except Exception as exp:
            # log.debug("exception while trying to connect to redis : {0} \n".format(exp))
            print "exception while trying to connect to redis : {0} \n".format(exp)
        return connection

    def set_val(self, key, value):
        conn = self.connect()
        try:
            conn.set(key, value)
        except Exception as exp:
            # log.debug("exception while set_value redis : {0} \n".format(exp))
            print "exception while set_value redis : {0} \n".format(exp)

    def get_val(self, key):
        conn = self.connect()
        try:
            offset = conn.get(key)
        except Exception as exp:
            # log.debug("exception while get_value redis : {0} \n".format(exp))
            print "exception while get_value redis : {0} \n".format(exp)

        return offset