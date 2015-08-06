from DataReceiver import DataReceiver
from DataSender import DataSender
from ChildDataSender import ChildDataSender
from SenderParent import *
from DataStoreConnection import *
from collections import deque
import sys, threading,time

# generic processor class - to use it, just create a subclass of Processor,
# which implements the function process(timestamp,[inval1,inval2...]) method
# 
# that function, when results are available, calls "addProcessedValues([outval1,outval2...])"
#
# if you want to output data at times other than when the data comes in (ie. in the process callback), then
# you can create a second thread, and call addProcessedValues in that
#
# If you have initialisation to do before starting, but after the arguments have been parsed, override
# processArguments(self,firstTimestamp)

# You don't need a main, just call Processor.run and it will read all arguments etc.
# it will even generate a usage explanation of what the arguments are if you run it with no arguments or not enough
#
# For multiple outputs it will use child data senders, so only one outputPort needed
#
# Arguments to the process are are of the form:
#
# inputHost1 inputPort1 inputHost2 inputPort2 ... outputPort arg1 arg2 arg3 ... hasGUI guiName
#
# also supports DB processing mode with -db as first parameter,
# 
# in db mode, first two params are a db address, start and duration (as seconds), then followed by stream IDs for the inputs (with optional time offset), then followed by stream names for the outputs, then args (in db mode, can't have a GUI)

class Processor():
    # inputs is a list of tuples [("input name","input description")]
    # outputs is either a list of tuples ("output default name",output description), or just a list of descriptions
    # for variable number of inputs, use * before the input name (and the arguments will be port host port host *)
    # arguments is a list of tuples [("arg name","arg description",<"default value">)]
    def __init__(self,processorName,inputs,outputs,arguments=[],guiSetup=None,supportsDBMode=True):
        self.processorName=processorName
        self.inputs=inputs
        self.numInputs=len(inputs)
        self.outputs=[]
        self.outputDefaultNames=[]
        for c in outputs:
            if type(c)==type(""):
                self.outputs.append(c)
                self.outputDefaultNames.append("")
            elif type(c)==type(()):
                self.outputs.append(c[1])
                self.outputDefaultNames.append(c[0])
            else:
                sys.stderr.write("Bad output type")
        self.numOutputs=len(outputs)
        self.arguments=[]
        self.argumentDefaultValues=[]
        for c in arguments:
            if len(c)==2:
                self.arguments.append(c)
                self.argumentDefaultValues.append("")
            elif len(c)==3:
                self.arguments.append((c[0],c[1]))
                self.argumentDefaultValues.append(c[2])
            else:
                sys.stderr.write("Bad argument type")
        self.numArguments=len(arguments)
        self.canHaveGui=False
        self.supportsDBMode=supportsDBMode
        self.guiSetup=guiSetup
        if guiSetup!=None:        
            self.canHaveGui=True
        self.numExpectedArgsMin=self.numInputs*2 + 1 +self.numArguments
        self.hasGUI=False
        self.processingLock=threading.Lock()
        
    def run(self):
        self.dbMode=False
        if len(sys.argv)>1 and sys.argv[1].lower()=="-xmlinfo":
            self.showXMLInfo()
            return
        if len(sys.argv)>1 and sys.argv[1].lower()=="-dbprocess":
            if not self.supportsDBMode:
                self.showUsage()
                return
            self.dbMode=True
            self.numExpectedArgsMin=5+self.numInputs + self.numOutputs +self.numArguments
        # parse arguments 
        if len(sys.argv)<self.numExpectedArgsMin+1:
            self.showUsage()
            return
        try:
            if self.dbMode:
                # read from database and process it into another db stream                
                curArg=2
                # get DB details
                self.dbHost=sys.argv[curArg]
                curArg+=1
                self.dbPort=int(sys.argv[curArg])
                
                curArg+=1
                self.dbStartTime=float(sys.argv[curArg])
                curArg+=1
                self.dbDuration=float(sys.argv[curArg])
                curArg+=1
                self.dbEndTime=self.dbStartTime+self.dbDuration
                print self.dbHost,self.dbPort
                self.dbConnection=DataStoreConnection(self.dbHost,int(self.dbPort))
                if not self.dbConnection.checkConnection():
                    print "Couldn't connect to database"
                    return -1
                # make receivers
                self.dataReceivers=[]
                self.inQueues=[]
                self.waitingData=[]
                self.lastValues=[]
                self.dbStreamIDs=[]
                for c in range(0,self.numInputs):                                               
                    if curArg>=len(sys.argv):
                        self.showUsage()
                        return
                    if self.inputs[c][0].startswith('*'):
                        while (not sys.argv[curArg].startswith('*')) and curArg<len(sys.argv):
                            # variable number of input sources, with a '*' at the end
                            inStream=int(sys.argv[curArg])
                            self.dbStreamIDs.append(inStream)
                            curArg+=1
                            newReceiver=DataReceiver("<cursor id='%d'/>@%s"%(inStream,self.dbHost),self.dbPort,self,len(self.dataReceivers))
                            self.dataReceivers.append(newReceiver)
                            self.inQueues.append(deque(maxlen=1000))                        
                            self.waitingData.append(False)
                            self.lastValues.append("0.0")
                        curArg+=1
                    else:
                        inStream=int(sys.argv[curArg])
                        self.dbStreamIDs.append(inStream)
                        curArg+=1
                        newReceiver=DataReceiver("<cursor id='%d'/>@%s"%(inStream,self.dbHost),self.dbPort,self,len(self.dataReceivers))
                        self.dataReceivers.append(newReceiver)
                        self.inQueues.append(deque(maxlen=1000))                        
                        self.lastValues.append("0.0")
                        self.waitingData.append(False)
                self.dbExistingSession=None
                self.dbSessionName="Processed data"
                for id in self.dbStreamIDs:
                    if self.dbExistingSession==None:
                        sessions=self.dbConnection.listSessions(int(id))
                        print sessions
                        if sessions!=None and len(sessions)>0:
                            self.dbExistingSession=int(sessions[0][0])
                            self.dbSessionName=None
                            print "Existing session:%d (%s)"%(self.dbExistingSession,sessions[0][1])
                    if self.dbStartTime==-1 or self.dbDuration==-1:
                        streamTimes=self.dbConnection.getStreamTimes([int(id)])
                        print streamTimes
                        if streamTimes!=None:
                            if self.dbStartTime==-1:
                                self.dbStartTime=streamTimes[0][1]
                            if self.dbDuration==-1:
                                self.dbEndTime=streamTimes[0][2]
                                self.dbDuration=streamTimes[0][2]-streamTimes[0][1]
                            self.dbEndTime=self.dbStartTime+self.dbDuration
                        
                # read the output stream names
                self.outputNames={}
                self.outputBuffers={}                
                self.outputStreams={}
                self.createStreamNames=[]
                for c in range(0,self.numOutputs):
                    if sys.argv[curArg].lower()!="none":
                        self.outputNames[c]=sys.argv[curArg]
                        self.outputBuffers[c]=deque(maxlen=512)
                        self.createStreamNames.append(sys.argv[curArg])
                    curArg+=1
                self.outputStreamsSorted=sorted(self.outputNames.items())
                self.outputSession=0
                self.outputStreamList=[]
                if len(self.outputNames)>0:
                    self.outputSession,self.outputStreamList=self.dbConnection.startSessionImporting(self.dbStartTime,self.createStreamNames,sessionName=self.dbSessionName,existingSessionID=self.dbExistingSession)
                for ((outCount,name),strmID) in zip(self.outputStreamsSorted,self.outputStreamList):
                    self.outputStreams[outCount]=int(strmID)

                # read the argument values
                self.argumentValues=[]
                for c in range(0,self.numArguments):
                    if curArg>=len(sys.argv):
                        self.showUsage()
                        return
                    self.argumentValues.append(sys.argv[curArg])
                    # if it is a float, make the argument value be a float
                    try:
                        self.argumentValues[-1]=float(self.argumentValues[-1])
                    except ValueError:
                        None
                    curArg+=1
                    
                # pretend that it is the db start time    
                self.processArguments(self.dbStartTime)

                # now start receivers and senders
                for (c,recv) in enumerate(self.dataReceivers):
                    recv.start()

                print "Connecting to database"
                doneConnection=False
                connectionAttempts=0
                while not doneConnection:
                    time.sleep(0.5)
                    doneConnection=True
                    for (c,recv) in enumerate(self.dataReceivers):
                        if recv.isConnected()==False:
                            doneConnection=False
                    connectionAttempts+=1                            
                    if connectionAttempts==5:
                        print "Error - couldn't connect receiver to database"
                        return -1
                for (c,recv) in enumerate(self.dataReceivers):                        
                    self.waitingData[c]=True
                    # fill initial buffers
                    recv.backchannelSendData("OFFSET %f 512"%(self.dbStartTime))
                self.dbTime=self.dbStartTime
                numPoints=0
                numWrites=0
                startProcessingTime=time.time()
                percentTime=(self.dbEndTime-self.dbStartTime)/100.0
                nextPercentTime=self.dbStartTime+percentTime
                percentage=0
                while self.dbTime<self.dbEndTime:
                    while self.dbTime>nextPercentTime:                    
                        percentage+=1
                        nextPercentTime+=percentTime
                        print "DBProcessing: %d%% complete,%d points read,%d points written"%(percentage,numPoints,numWrites)
                        sys.stdout.flush()
                    minTime=self.dbEndTime
                    minQueue=None
                    emptyQueue=False
                    minQueueNum=None
                    # find the earliest datapoint
                    for c,queue in enumerate(self.inQueues):
                        if len(queue)==0 and self.waitingData[c]==True:
                            # if one of the queues is empty and waiting for more data, we have to wait for it to be filled
                            # before we can work out what is the shortest point
                            print "Waiting data"
                            time.sleep(0.1)
                            emptyQueue=True
                            minQueue=None
                            break
                        elif len(queue)>0 and queue[0][0]<minTime:
                            minTime=min(minTime,queue[0][0])
                            minQueue=queue
                            minQueueNum=c
                    if minQueue!=None and minTime<self.dbEndTime:
                        # process this point
                        timestamp,value=minQueue.popleft()
                        self.lastValues[minQueueNum]=value
                        self.dbTime=timestamp
                        self.process(timestamp,self.lastValues,minQueueNum)
                        numPoints+=1
#                        print "Process: ",timestamp,self.lastValues
                    # get more data if needed
                    for c,queue in enumerate(self.inQueues):
                        if len(queue)<256 and len(queue)>0 and self.waitingData[c]==False:
                            lastTime=queue[-1][0]
                            self.waitingData[c]=True
                            self.dataReceivers[c].backchannelSendData("OFFSET %f %d"%(lastTime,999-len(queue)))   
                    # write output buffers if they are big enough
                    for streamNum,buffer in self.outputBuffers.items():
                        if ((not emptyQueue) and minTime>=self.dbEndTime and len(buffer)>0) or len(buffer)>=256:
                            numWrites+=len(buffer)
                            self.dbConnection.writeTimestampedImportData(self.outputStreams[streamNum],buffer)
                            buffer.clear()
                    if (not emptyQueue) and minTime>=self.dbEndTime:
                        # no data left
                        break
                print "DBProcessing: 100%% complete,%d points read,%d points written"%(numPoints,numWrites)
                print "Done processing, %d data points read, %d data points written, %.2f seconds"%(numPoints,numWrites,time.time()-startProcessingTime)
                sys.stdout.flush()
                for c in self.dataReceivers:
                    c.stopReceiving()
                    # receivers will only stop once some data comes to them, so force it with a backchannel send
                    c.backchannelSendData("OFFSET 0 0")   
                print "Ended receivers"
                return                
                    
                # we're saving alright, so now step through all the db streams point by point, getting values greater than the current value
                # we have to do cunning things in receive callbacks to fill queues
                #
                # first: seek all dbcursors to the start time and return a buffer full forwards (which goes into a queue for that stream)
                # take the earliest point off any of the receive stream queues
                # update last values and call process with that timestamp
                #
                # in db mode, addProcessedValues should add the current timestamp
                # 
                # if a stream buffer is > half empty, send a request to refill
                # if a stream buffer is empty, wait till request ends on that stream
                # if the request ends and the buffer is empty, mark that stream as done (replace it with none)
                # 
            else:
                # normal live mode
                curArg=1
                # make receivers
                self.dataReceivers=[]
                self.lastValues=[]            
                for c in range(0,self.numInputs):
                    if curArg>=len(sys.argv):
                        self.showUsage()
                        return
                    if self.inputs[c][0].startswith('*'):
                        while (not sys.argv[curArg].startswith('*')) and curArg<len(sys.argv):
                            # variable number of input sources, with a '*' at the end
                            inHost=sys.argv[curArg]
                            curArg+=1
                            inPort=int(sys.argv[curArg])
                            curArg+=1
                            newReceiver=DataReceiver(inHost,inPort,self,len(self.dataReceivers))
                            self.dataReceivers.append(newReceiver)
                            self.lastValues.append("0.0")                        
                        curArg+=1
                    else:
                        inHost=sys.argv[curArg]
                        curArg+=1
                        inPort=int(sys.argv[curArg])
                        curArg+=1
                        newReceiver=DataReceiver(inHost,inPort,self,len(self.dataReceivers))
                        self.dataReceivers.append(newReceiver)
                        self.lastValues.append("0.0")
                        
                outPort=int(sys.argv[curArg])
                curArg+=1
                # make sender(s) and queues
                self.dataSenders=[]
                if self.numOutputs==1:
                    self.dataSenders.append(DataSender(outPort, self))
                else:
                    self.senderParent=SenderParent(outPort,self)
                    for c in range(0,self.numOutputs):
                        self.dataSenders.append (ChildDataSender(c,self))
                    
                self.argumentValues=[]
                for c in range(0,self.numArguments):
                    if curArg>=len(sys.argv):
                        self.showUsage()
                        return
                    self.argumentValues.append(sys.argv[curArg])
                    # if it is a float, make the argument value be a float
                    try:
                        self.argumentValues[-1]=float(self.argumentValues[-1])
                    except ValueError:
                        None
                    curArg+=1
                if self.canHaveGui:
                    if curArg<len(sys.argv):
                        if sys.argv[curArg]=="1" or sys.argv[curArg].lower()=="true" or sys.argv[curArg].lower()=="yes":
                            self.hasGUI=True
                            self.guiTitle="%s [%d]"%(self.processorName,outPort)
                            curArg+=1
                            if curArg<len(sys.argv):
                                self.guiTitle=sys.argv[curArg]
                                curArg+=1                                
                self._initialClock=time.clock()
                self._initialTime=time.time()
                self.processArguments(self._initialTime)

                # now start receivers and senders
                for c in self.dataReceivers:
                    c.start()
                for c in self.dataSenders:
                    c.setPush(True)
                    c.start()
                if self.numOutputs>1:
                    self.senderParent.start()
                                        
                # now show gui if needed (this blocks until the gui is closed)
                if self.hasGUI:
                    self.runGUI(self.guiSetup,self.guiTitle)
                else:
                    # now just hold on - processing is done in response to the receivers calling setData
                    # and output is done when the process function calls addProcessedValues
                    while True:
                        time.sleep(1000.0)
                                
        except IOError,e:
            print e
            self.showUsage()
            return

    def showXMLInfo(self):
        print "<processorInfo name='%s' gui='%s' dbSupport='%s'>"%(self.processorName,self.canHaveGui,self.supportsDBMode)
        print "  <inputs>"
        for (name,desc) in self.inputs:
            if name.startswith('*'):
                print "    <input name='%s' multiple='true'>%s</input>"%(name[1:],desc)
            else:
                print "    <input name='%s' multiple='false'>%s</input>"%(name,desc)
        print "  </inputs>"
        print "  <outputs>"
        for (c,(desc,defaultName)) in enumerate(zip(self.outputs,self.outputDefaultNames)):
            print "    <output id='%d' defaultName='%s'>%s</output>"%(c,defaultName,desc)
        print "  </outputs>"
        print "  <arguments>"
        for ((name,desc),defaultValue) in zip(self.arguments,self.argumentDefaultValues):
            print "    <argument name='%s' default='%s'>%s</argument>"%(name,defaultValue,desc)
        print "  </arguments>"
        print "</processorInfo>"


    def showUsage(self):
        print "Usage: \n  %s"%self.processorName,
        for (name,desc) in self.inputs:
            if name.startswith('*'):            
                print "<%s Host1> <%s Port1> <%s Host2> <%s Port2> ... * "%(name[1:],name[1:],name[1:],name[1:]),
            else:
                print "<%s Host> <%s Port>"%(name,name),
        print "<outport>",
        for (name,desc) in self.arguments:
            print "<%s>"%name,
        if self.canHaveGui:
            print "[<showGui> <guiName>]",
        print "\n\n",
        if self.supportsDBMode:
            print "Or:\n  %s -dbprocess <dbHost> <dbPort> <startTime> <duration>"%self.processorName,
            for (name,desc) in self.inputs:
                if name.startswith('*'):            
                    print "<%s dbStream1> <%s dbStream2> ... * "%(name[1:],name[1:]),
                else:
                    print "<%s db Stream> "%(name),
            for c in range(len(self.outputs)):
                print "<output stream %d name>"%c
            for (name,desc) in self.arguments:
                print "<%s>"%name,
            print "\n\n",
        
        print "Arguments:"
        for (name,desc) in self.inputs:            
            numSpaces=0
            if name.startswith('*'):            
                if len(name)<8:
                    numSpaces=8-len(name)
                print " <%s host/port/dbStream(n)> "%name[1:]+(" "*(numSpaces)),
                print desc,"(end the list with a single argument '*')"
            else:
                if len(name)<9:
                    numSpaces=9-len(name)
                print " <%s host/port/dbStream> "%name+(" "*(numSpaces)),
                print desc        
        print " <outport>                       Port to output processed data"
        for (name,desc) in self.arguments:
            numSpaces=0
            if len(name)<28:
                numSpaces=28-len(name)
            print " <%s> "%name+(" "*(numSpaces)),
            print desc        
        if self.canHaveGui:
            print " <showGui>                       True (or yes, or 1) if you want to show the GUI"
            print " <guiName>                       Title of the GUI window"
        if self.supportsDBMode:
            print " -dbprocess                      Run this processor on a database stream"
            print " <dbHost>                        Host name that the database is running on"
            print " <dbPort>                        Port that the database is running on"
            print " <startTime>                     Start time for database processing (or -1 for start of stream)"
            print " <duration>                      Duration of database processing (or -1 to process till end)"
            print " <output stream name>            Name of the output stream in the database"
        print ""
        if self.numOutputs==1:
            print "Output:      %s"%self.outputs[0]
        elif self.numOutputs>1:
            print "Outputs are:"
            for c in range(len(self.outputs)):
                print " Channel %d:    %s"%(c,self.outputs[c])
        sys.exit(-1)
            
    # if we only have one output, then the argument is that output.
    # otherwise, for multiple outputs,this is a list of values, which are put into the respective senders by order
    # or a dictionary where the keys are sender numbers, to only update some senders
    def addProcessedValues(self,values):
        if self.dbMode:
            if self.numOutputs==1:
                # single value - go to first sender
                if self.outputBuffers.has_key(0):
                    self.outputBuffers[0].append((self.dbTime,str(values)))
            elif type(values)==type([]):
                # list of values 1 per sender
                if len(values)!=self.numOutputs:
                    print "Wrong number of output values in %s"%self.processorName
                    return
                for c in range(0,self.numOutputs):
                    if self.outputBuffers.has_key(c):
                        self.outputBuffers[c].append((self.dbTime,str(values[c])))
            elif type(values)==type({}):
                # dict of values where keys are the sender number
                for (key,value) in values.iteritems():
                    if type(key)!=type(1) or key<0 or key>=self.numOutputs:
                        print "Bad output number %s in %s"%(str(key),self.processorName)
                        return
                    if self.outputBuffers.has_key(key):
                        self.outputBuffers[key].append((self.dbTime,str(value)))            
        else:
            if self.numOutputs==1:
                # single value - go to first sender
                if self.dataSenders[0].isConnected():
                    self.dataSenders[0].pushData(str(values))
            elif type(values)==type([]):
                # list of values 1 per sender
                if len(values)!=self.numOutputs:
                    print "Wrong number of output values in %s"%self.processorName
                    return
                for c in range(0,self.numOutputs):
                    if self.dataSenders[c].isConnected():
                        self.dataSenders[c].pushData(str(values[c]))
            elif type(values)==type({}):
                # dict of values where keys are the sender number
                for (key,value) in values.iteritems():
                    if type(key)!=type(1) or key<0 or key>=self.numOutputs:
                        print "Bad output number %s in %s"%(str(key),self.processorName)
                        return
                    if self.dataSenders[key].isConnected():
                        self.dataSenders[key].pushData(str(value))
    
    def getMultiStream(self,queryString):
        print "connect multistream %s"%queryString
        try:
            if int(queryString)<self.numOutputs and int(queryString)>=0:
                return self.dataSenders[int(queryString)]
        except ValueError,v:
            print "bad query string for %s"%self.processorName
            return None
            
    # called by receiver threads    
    def setData(self, data,portNumber):
        if  self.dbMode:
            if data.startswith('END,'):
                self.waitingData[portNumber]=False
            else:
                splitPos=data.find(',')
                if splitPos>=0:
                    timestamp=data[0:splitPos]
                    value=data[splitPos+1:]
                    self.inQueues[portNumber].append((float(timestamp),value))                
        else:
            self.lastValues[portNumber]=data
            # make sure only one thread is inside process at any one time
            # or else things get too complicated
            with self.processingLock:
                curTime=time.clock() - self._initialClock+self._initialTime
                self.process(curTime,self.lastValues,portNumber)            

    # processArguments is called once the arguments have been parsed, but before the processing starts, to allow initialisation        
    def processArguments(self,firstTimestamp):
        None
        
    def process(self,timeStamp,values,queue):
        #the queue is passed to allow a single dominant stream to trigger processing (useful for windowed stuff)
        print "Processor process function being called - should be overridden by subclass"
            
    # run the GUI - note that this is done last, as it takes over the main thread
    def runGUI(self, GUISetup, title="Processor"):
        import wx
        app = wx.App(False)

        frame = wx.Frame(None,title=title)
        frame.Show(True)
        GUISetup(frame)
        app.MainLoop()

