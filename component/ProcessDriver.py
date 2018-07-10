from ProcessEngine import ProcessEngine
from ProcessEngineSQL import ProcessEngineSQL
from GreenplumEngine import GreenplumEngine
class ProcessDriver():
    def __init__(self,appName,processConfig):
        self.__appName=appName
        self.__processConfig=processConfig
    def startCore(self):
        pe=ProcessEngine(self.__appName)
        pe.execute(self.__processConfig)
    def startSQL(self):
        pe=ProcessEngineSQL(self.__appName)
        pe.execute(self.__processConfig)
    def startgpSQL(self):
        pe=GreenplumEngine(self.__appName)
        pe.execute(self.__processConfig)