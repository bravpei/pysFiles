from pyspark.sql import SparkSession
class ProcessEngineSQL():
    def __init__(self,appName):
        self.__df=None
        self.__spark = SparkSession.Builder().appName(appName).getOrCreate()
        self.__jdbcUrl=None
        self.__properties=None
    def execute(self,processConfig):
        for head in processConfig:
            if(head["nodeName"])=="init":
                self.__init(head)
            elif(head["nodeName"])=="query":
                self.__df=self.__query(head)
            elif(head["nodeName"])=="saveToTable":
                self.__saveToTable(head)
    def __init(self,nodeMap):
        self.__jdbcUrl=nodeMap["jdbcUrl"]
        self.__properties={"user":nodeMap["user"],"password":nodeMap["password"]}
    def __query(self,nodeMap):
        names=nodeMap["tableName"].split(",")
        for name in names:
            self.__spark.read.jdbc(self.__jdbcUrl,name,properties=self.__properties).createOrReplaceTempView(name)
        return self.__spark.sql(nodeMap["sql"])
    def __saveToTable(self,nodeMap):
        self.__df.write.mode("append").jdbc(self.__jdbcUrl,nodeMap["tableName"],properties=self.__properties)