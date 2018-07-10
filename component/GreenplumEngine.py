import psycopg2
from pyspark.sql import SparkSession
class GreenplumEngine():
    def __init__(self,appName):
        self.__spark = SparkSession.Builder().appName(appName).getOrCreate()
        self.__database=None
        self.__user=None
        self.__password=None
        self.__host=None
        self.__port=None
        self.__connect=None
        self.__cursor=None
    def execute(self, processConfig):
        for head in processConfig:
            if (head["nodeName"]) == "init":
                self.__init(head)
            elif (head["nodeName"]) == "execute":
                self.__execute(head)
            elif (head["nodeName"]) == "closeConnect":
                self.__close(head)
    def __init(self,nodeMap):
        self.__database=nodeMap["database"]
        self.__user=nodeMap["user"]
        self.__password=nodeMap["password"]
        self.__host=nodeMap["host"]
        self.__port=nodeMap["port"]
        self.__connect=psycopg2.connect(database=self.__database,user=self.__user,password=self.__password,host=self.__host,port=self.__port)
        self.__cursor=self.__connect.cursor()
    def __execute(self,nodeMap):
        try:
            self.__cursor.execute(nodeMap["gpsql"])
            self.__connect.commit()
        except:
            self.__connect.rollback()
    def __close(self,nodeMap):
        if(nodeMap["isClose"]=="1"):
            self.__cursor.close()
            self.__connect.close()
            self.__spark.stop()