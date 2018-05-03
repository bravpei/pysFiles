import sys
sys.path.append(r"/opt/test/component/ProcessEngine")
from component.ProcessEngine import ProcessEngine
class ProcessDriver():
    def __init__(self,appName,processConfig):
        self.__appName=appName
        self.__processConfig=processConfig
    def start(self):
        pe=ProcessEngine(self.__appName)
        pe.execute(self.__processConfig)