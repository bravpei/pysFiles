from pyspark import SparkContext,SparkConf
class ProcessEngine():
    def __init__(self,appName):
        self.__rdd=None
        self.__appName=appName
    def execute(self,processConfig):
        for head in processConfig:
            if head["nodeName"]=="init":
                self.__rdd=self.__init(head)
            elif head["nodeName"]=="flatMap":
                self.__rdd= self.__flatMap(head,self.__rdd)
            elif head["nodeName"]=="map":
                self.__rdd= self.__map(head,self.__rdd)
            elif head["nodeName"]=="reduceByKey":
                self.__rdd= self.__reduceByKey(head,self.__rdd)
            elif head["nodeName"]=="filter":
                self.__rdd= self.__filter(head,self.__rdd)
            elif head["nodeName"]=="saveAsTextFile":
                self.__rdd =self.__saveAsTextFile(head,self.__rdd)
            elif head["nodeName"]=="coalesce":
                self.__rdd = self.__coalesce(head,self.__rdd)
            elif head["nodeName"]=="glom":
                self.__rdd =self.__glom(self.__rdd)
            elif head["nodeName"]=="groupBy":
                self.__rdd = self.__groupBy(head,self.__rdd)
            elif head["nodeName"]=="groupByKey":
                self.__rdd = self.__groupByKey(head,self.__rdd)
            elif head["nodeName"]=="keyBy":
                self.__rdd = self.__keyBy(head,self.__rdd)
            elif head["nodeName"]=="keys":
                self.__rdd = self.__keys(head,self.__rdd)
            elif head["nodeName"]=="mapPartitions":
                self.__rdd = self.__mapPartitions(head,self.__rdd)
            elif head["nodeName"]=="mapPartitionsWithIndex":
                self.__rdd = self.__mapPartitionsWithIndex(head,self.__rdd)
            elif head["nodeName"]=="mapPartitionsWithSplit":
                self.__rdd = self.__mapPartitionsWithSplit(head,self.__rdd)
            elif head["nodeName"]=="mapValues":
                self.__rdd = self.__mapValues(head,self.__rdd)
            elif head["nodeName"]=="partitionBy":
                self.__rdd = self.__partitionBy(head,self.__rdd)
            elif head["nodeName"]=="persist":
                self.__rdd = self.__persist(head,self.__rdd)
            elif head["nodeName"]=="repartition":
                self.__rdd = self.__repartition(head,self.__rdd)
            elif head["nodeName"]=="repartitionAndSortWithinPartitions":
                self.__rdd = self.__repartitionAndSortWithinPartitions(head,self.__rdd)
            elif head["nodeName"]=="sample":
                self.__rdd = self.__sample(head,self.__rdd)
            elif head["nodeName"]=="sampleByKey":
                self.__rdd = self.__sampleByKey(head,self.__rdd)
            elif head["nodeName"]=="sortBy":
                self.__rdd = self.__sortBy(head,self.__rdd)
            elif head["nodeName"]=="sortByKey":
                self.__rdd = self.__sortByKey(head,self.__rdd)
            elif head["nodeName"]=="values":
                self.__rdd = self.__values(self.__rdd)
    def __init(self,nodeMap):
        conf=SparkConf().setAppName(self.__appName)
        return SparkContext(conf=conf).textFile(nodeMap["path"])
    def __flatMap(self,nodeMap,rdd):
        return rdd.flatMap(eval(nodeMap["operation"]))
    def __map(self,nodeMap,rdd):
        return rdd.map(eval(nodeMap["operation"]))
    def __reduceByKey(self,nodeMap,rdd):
        return rdd.reduceByKey(eval(nodeMap["operation"]))
    def __filter(self,nodeMap,rdd):
        return rdd.filter(eval(nodeMap["operation"]))
    def __saveAsTextFile(self,nodeMap,rdd):
        import time
        rdd.saveAsTextFile(nodeMap["operation"]+"/"+self.__appName+"-"+time.strftime('%Y%m%d%H%M%S',time.localtime(time.time())))
    def __coalesce(self,nodeMap,rdd):
        return rdd.coalesce(eval(nodeMap["operation"]))
    def __glom(self,rdd):
        return rdd.glom()
    def __groupBy(self,nodeMap,rdd):
        return rdd.groupBy(eval(nodeMap["operation"]))
    def __groupByKey(self,nodeMap,rdd):
        return rdd.groupByKey(eval(nodeMap["operation"]))
    def __keyBy(self,nodeMap,rdd):
        return rdd.keyBy(eval(nodeMap["operation"]))
    def __keys(self,nodeMap,rdd):
        return rdd.keys(eval(nodeMap["operation"]))
    def __mapPartitions(self,nodeMap,rdd):
        return rdd.mapPartitions(eval(nodeMap["operation"]))
    def __mapPartitionsWithIndex(self,nodeMap,rdd):
        return rdd.mapPartitionsWithIndex(eval(nodeMap["operation"]))
    def __mapPartitionsWithSplit(self,nodeMap,rdd):
        return rdd.mapPartitionsWithSplit(eval(nodeMap["operation"]))
    def __mapValues(self,nodeMap,rdd):
        return rdd.mapValues(eval(nodeMap["operation"]))
    def __partitionBy(self,nodeMap,rdd):
        return rdd.partitionBy(eval(nodeMap["operation"]))
    def __persist(self,nodeMap,rdd):
        return rdd.persist(eval(nodeMap["operation"]))
    def __repartition(self,nodeMap,rdd):
        return rdd.repartition(eval(nodeMap["operation"]))
    def __repartitionAndSortWithinPartitions(self,nodeMap,rdd):
        return rdd.repartitionAndSortWithinPartitions(eval(nodeMap["operation"]))
    def __sample(self,nodeMap,rdd):
        return rdd.sample(eval(nodeMap["operation"]))
    def __sampleByKey(self,nodeMap,rdd):
        return rdd.sampleByKey(eval(nodeMap["operation"]))
    def __sortBy(self,nodeMap,rdd):
        return rdd.sortBy(eval(nodeMap["operation"]))
    def __sortByKey(self,nodeMap,rdd):
        return rdd.sortByKey(eval(nodeMap["operation"]))
    def __values(self,rdd):
        return rdd.values()