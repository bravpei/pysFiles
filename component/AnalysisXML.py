from component.ProcessDriver import ProcessDriver
from hdfs.client import Client
import xml.etree.ElementTree as ET
import sys
client=Client("http://172.18.130.100:50070")
with client.read("/DA/AnalysisXML/newProcess_111.xml") as fs:
    list=[]
    key=""
    value=""
    tree=ET.parse(fs)
    root=tree.getroot()
    appName=root.attrib["appName"]
    for childs in root:
        map={}
        for child in childs:
            if child.tag=="key":
                key=child.text
            elif child.tag=="value":
                value=child.text
                map[key]=value
        list.append(map)
    type=list[0]['type']
    if(type=="core"):
        pd = ProcessDriver(appName, list)
        pd.startCore()
    elif(type=="sql"):
        pd = ProcessDriver(appName, list)
        pd.startSQL()