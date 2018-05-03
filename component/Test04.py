import sys
sys.path.append(r"/opt/test/component")
from component.ProcessDriver import ProcessDriver
from hdfs.client import Client
import xml.etree.ElementTree as ET
client=Client("http://172.18.130.100:50070")
with client.read("/liupei/test/template.xml") as fs:
    list=[]
    key=""
    value=""
    #tree=ET.parse("/home/liupei/test/template.xml")
    tree=ET.parse(fs)
    root=tree.getroot()
    appName=root.attrib["appName"]
    #print(appName)
    for childs in root:
        map={}
        for child in childs:
            if child.tag=="key":
                key=child.text
            elif child.tag=="value":
                value=child.text
                map[key]=value
        list.append(map)
    #print(list)
    pd = ProcessDriver(appName, list)
    pd.start()