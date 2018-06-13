from pyspark import SparkContext
from pyspark.streaming import StreamingContext
class ProcessEngineStreaming():
    def __init__(self,appName):
        self.__dsrdd=None
        self.__appName=appName
    def execute(self,processConfig):
        for head in processConfig:
            if(head["nodeName"])=="initSocket":
                self.__dsrdd=self.__initSocket(head)
    def __initSocket(self,nodeMap):
        sc=SparkContext("yarn",self.__appName)
        ssc=StreamingContext(sc,nodeMap["batchDuration"])
        return  ssc.socketTextStream(nodeMap["ip"],nodeMap["port"])