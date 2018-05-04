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
                self.__saveAsTextFile(head,self.__rdd)
            elif head["nodeName"]=="coalesce":
                self.__coalesce(head,self.__rdd)
            elif head["nodeNanName"]=="glom":
                self.__glom(self.__rdd)
            elif head["nodeNanName"]=="groupBy":
                self.__groupBy(head,self.__rdd)
            elif head["nodeNanName"]=="groupByKey":
                self.__groupByKey(head,self.__rdd)
            elif head["nodeNanName"]=="keyBy":
                self.__keyBy(head,self.__rdd)
            elif head["nodeNanName"]=="keys":
                self.__keys(head,self.__rdd)
            elif head["nodeNanName"]=="mapPartitions":
                self.__mapPartitions(head,self.__rdd)
            elif head["nodeNanName"]=="mapPartitionsWithIndex":
                self.__mapPartitionsWithIndex(head,self.__rdd)
            elif head["nodeNanName"]=="mapPartitionsWithSplit":
                self.__mapPartitionsWithSplit(head,self.__rdd)
            elif head["nodeNanName"]=="mapValues":
                self.__mapValues(head,self.__rdd)
            elif head["nodeNanName"]=="partitionBy":
                self.__partitionBy(head,self.__rdd)
            elif head["nodeNanName"]=="persist":
                self.__persist(head,self.__rdd)
            elif head["nodeNanName"]=="repartition":
                self.__repartition(head,self.__rdd)
            elif head["nodeNanName"]=="repartitionAndSortWithinPartitions":
                self.__repartitionAndSortWithinPartitions(head,self.__rdd)
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