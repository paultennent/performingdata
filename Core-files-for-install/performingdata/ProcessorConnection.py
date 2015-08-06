# use this for running processors remotely (eg. to run a processor on a database stream or something)
# eg. it is used in the renderer to handle running processors to make db streams

# processors are run as separate processes, both for efficiency, and also for
# the ability to run non-python processors if anyone writes one

import os
import re
import subprocess
import xml.dom.minidom 

class ProcessorConnection:
    def __init__(self,processorPath=os.path.dirname(__file__),processorRegExp="\w+Processor.(exe|py|class)"):
        self.processorPath=processorPath
        self.processorRegexp=re.compile(processorRegExp)
        
    def listProcessors(self):
        allFiles=os.listdir(self.processorPath)
        processorList=[]
        for filename in allFiles:
            if self.processorRegexp.match(filename)!=None:
                processorList.append(filename)
        return processorList
        
    def getText(self,nodelist):
        rc = []
        for node in nodelist:
            if node.nodeType == node.TEXT_NODE:
                rc.append(node.data)
        return ''.join(rc)
        
    def getProcessorSpawnPath(self,processorName):
        processorFullName=os.path.join(self.processorPath,processorName)
        if processorName.lower().endswith(".exe"):
            return '"%s"'%processorFullName
        elif processorName.lower().endswith(".py"):
            return 'python "%s"'%processorFullName
        elif processorName.lower().endswith(".class"):
            return 'java "%s"'%processorFullName
        else:
            print "Don't know how to run processor ",processorName
            return None
                
    def getProcessorInfo(self,processorName):
        retVal={}
        spawnPath=self.getProcessorSpawnPath(processorName)
        if spawnPath!=None:
            output=subprocess.check_output(spawnPath+" -xmlinfo")
            if output!=None:
                document=xml.dom.minidom.parseString(output)
                inputs=document.getElementsByTagName("input")
                inputList=[]
                for input in inputs:
                    inputList.append((input.getAttribute("name"),self.getText(input.childNodes)))
                outputs=document.getElementsByTagName("output")
                outputList=[]
                for output in outputs:
                    outputList.append((output.getAttribute("id"),self.getText(output.childNodes),output.getAttribute("defaultName")))
                arguments=document.getElementsByTagName("argument")
                argumentList=[]
                for argument in arguments:
                    argumentList.append((argument.getAttribute("name"),self.getText(argument.childNodes),argument.getAttribute("default")))
                retVal["inputs"]=inputList
                retVal["outputs"]=outputList
                retVal["arguments"]=argumentList
                hasGUI=document.documentElement.getAttribute("gui")
                if hasGUI.lower()=="true" or hasGUI.lower()=="yes" or hasGUI.lower()=="1":
                    retVal["gui"]=True
                else:
                    retVal["gui"]=False
                dbSupport=document.documentElement.getAttribute("dbSupport")
                if dbSupport.lower()=="true" or dbSupport.lower()=="yes" or dbSupport.lower()=="1":
                    retVal["db"]=True
                else:
                    retVal["db"]=False
                return retVal
        return None

    def processDatabaseStreams(self,processorName,dbHost,dbPort,dbStartTime,dbDuration,inputs,outputs,args,progressCallback=None):
        spawnPath=self.getProcessorSpawnPath(processorName)
        if spawnPath!=None:
            spawnPath+=" -dbProcess %s %s %s %s"%(dbHost,dbPort,dbStartTime,dbDuration)
            for c in inputs:
                spawnPath+=" %s"%c
            for c in outputs:
                spawnPath+=" %s"%c
            for c in args:
                spawnPath+=" %s"%c
            p = subprocess.Popen(spawnPath, bufsize=0,
                 stdout=subprocess.PIPE)
            while True:
                line = p.stdout.readline()
                if not line or len(line)==0:
                    break
                match=re.match("DBProcessing: (\d+).*?,(\d+).*?,(\d+).*",line)
                if match!=None:
                    if progressCallback!=None:
                        progressCallback(*map(int,match.groups()))
                    if int(match.groups()[0])==100:
                        break
    
if __name__ == '__main__':
    def progressShower(percentage,numpoints,numwrites):
        print "%d percent, %d read, %d written"%(percentage,numpoints,numwrites)

    c=ProcessorConnection()
#    print c.listProcessors()
    print c.getProcessorInfo("thresholdProcessor.py")
    c.processDatabaseStreams("gsrprocessor.py","127.0.0.1",12345,-1,10,[550],['hrOut'],[],progressShower)
