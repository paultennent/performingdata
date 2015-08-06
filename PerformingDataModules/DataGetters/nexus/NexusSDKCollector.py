
#get data from a nexus biofeedback box - needs biotrace running etc. JM:beware - I just hacked this up for testing, no guarantees!

#from ChildDataSender import ChildDataSender
import serial
from collections import deque
import sys, random,time
from performingdata.SenderParent import *
import struct

from ctypes import *



class NexusSDKCollector():
    
    def run(self,nexusSerial,port):
        self.despikes=[0,0,0]
    
        self.portBase=port
        # set up senders and data queues
        self.outQueues = []
        self.dataSenders=[]
        self.senderParent=SenderParent(self.portBase,self)
        for c in range(0,10):
            self.dataSenders.append (ChildDataSender(c,self))
            self.outQueues.append(deque())
        for ds in self.dataSenders:
            ds.start()
        self.senderParent.start()
    
    
        NEXUSDATAFUNC = CFUNCTYPE(None,c_int, c_int, POINTER(c_float))
        self.dll=cdll.LoadLibrary('GenericDeviceInterfaceDLL2.dll')
        self.dll.InitGenericDevice.restype=c_int
        self.dll.StartGenericDevice.restype=c_int
        self.callbackFunction=NEXUSDATAFUNC(self.nexusDataFunction)
        retVal=self.dll.InitGenericDevice(self.callbackFunction,1,c_longlong(nexusSerial))    
        if retVal==-6:
            self.dll.ShowAuthenticationWindow()
                    
        print retVal
        sampleRate=c_int(256)
        retVal=self.dll.StartGenericDevice(byref(sampleRate))
        print retVal
        
        while True:
            time.sleep(10)
    
    def nexusDataFunction(self,nsamples,nchannels,pData):                
        if nchannels>=10:
            data=[0,0,0,0,0,0,0,0,0,0]
            for c in range(10):
                data[c]=pData[c]            
            self.despikeGSR(data)
            for (c,ds) in enumerate(self.dataSenders):
                if ds.isConnected():
                    self.outQueues[c].append("%f"%data[c])
            # trigger the data senders all at once
            for ds in self.dataSenders:
                if ds.isConnected():
                    ds.dataReady()
        #print "[%2.2d,%2.2d] "%(nsamples,nchannels),
         
        #for c in range(nchannels):
            #print "%2.2f"%pData[c],
            #print ",",
        #print ""
        
    
    def despikeGSR(self,data):
        self.despikes.append(data[5])
        if len(self.despikes)>3:
            self.despikes=self.despikes[1:]
        minVal=min(self.despikes)
        maxVal=max(self.despikes)
        curVal=data[5]
        for c in self.despikes:
            if c!=minVal and c!=maxVal:
                data[5]=c
        
        
                    
    def getData(self,port):
        portnum=port
        if len(self.outQueues[portnum])>0:
            return self.outQueues[portnum].popleft()
        else:
            return None
            
    def getMultiStream(self,queryString):
        print "connect multistream %s"%queryString
        try:
            if int(queryString)<len(self.outQueues) and int(queryString)>=0:
                return self.dataSenders[int(queryString)]
        except ValueError,v:
            None
        print "bad query string for nexus collector"
        return None
        
def main(serialNum,portBase):
    c = NexusSDKCollector()
    c.run(serialNum,portBase)
    
if __name__ == '__main__': 
    if len(sys.argv)==3:
        main(int(sys.argv[1]),int(sys.argv[2]))
    else:
        print ""
        print "usage: NexusSDKCollector <serial number> <Network port>"
        print "      <serial number>      Nexus serial number (eg. "
        print "      <Network port>  Network port for Vicarious stuff to connect to (eg. 49990)"
        print ""
      
