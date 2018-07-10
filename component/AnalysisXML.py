from ProcessDriver import ProcessDriver
from hdfs.client import Client
import xml.etree.ElementTree as ET
import sys
client=Client(sys.argv[1])
with client.read(sys.argv[2]) as fs:
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
    type=list[0]["type"]
    pd = ProcessDriver(appName, list)
    if(type=="core"):
        pd.startCore()
    elif(type=="sql"):
        pd.startSQL()
    elif(type=="gpsql"):
        pd.startgpSQL()