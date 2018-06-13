import sys
sys.path.append(r"/opt/test/component/ProcessEngine")
from component.ProcessEngine import ProcessEngine
from component.ProcessEngineSQL import ProcessEngineSQL
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
