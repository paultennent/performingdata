from performingdata.ChildDataSender import ChildDataSender
from collections import deque
import sys, random,time,signal,atexit,os
from performingdata.VilistusReader import *
from performingdata.SenderParent import *
import serial

class VilistusSerialCollector():
    
    def run(self,sensorPort,portBase):
        self.comport=None
        self.portBase = portBase
        self.outQueues = []
        self.dataSenders=[]
        self.portCount=6
        self.senderParent=SenderParent(portBase,self)
        for c in range(0,self.portCount):
            self.dataSenders.append (ChildDataSender(c,self))
            self.outQueues.append(deque([],1000))
        for ds in self.dataSenders:
            ds.start()
        self.senderParent.start()
        
        self.connected=False
        self.comport=None
        
        dataBuffers=[]
        spareData=""
        
        while(True):
            try:
                # connect to vilistus if it isn't
                if not self.connected:                
                    time.sleep(0.25)
                    self.comport=serial.Serial(sensorPort,57600,timeout=5)
                    dataLines=self.comport.readlines(64)
                    if len(dataLines)==0 or len(dataLines[-1])<20:                    
                        print "Can't connect to q sensor %s"%sensorPort
                        self.connected=False
                        self.comport.close()
                        self.comport=None
                    else:
                        self.connected=True
                        print "connected to q sensor %s"%sensorPort
                        self.comport.timeout=1.0
                else:
                    dataLine=self.comport.readline(64)
                    dataLine=dataLine.strip('\r')
                    dataLineSplit=dataLine.split(',')
                    if len(dataLineSplit)>=7:
                        #print "Good data Line",dataLine
                        for c in range(0,self.portCount):
                            try:
                                value=float(dataLineSplit[c+1])                                
                                if self.dataSenders[c].isConnected():
                                    self.outQueues[c].append("%f"%value)
                            except ValueError:
                                print "Bad value:",dataLineSplit[c+1]
                        # trigger the data senders
                        for ds in self.dataSenders:
                            ds.dataReady()
                    else:
                        print "Bad data line",dataLine
                        self.comport.close()
                        self.comport=None
                        self.connected=False
            except serial.SerialException,e:
                print "Serial connection failed",e
                self.comport=None
                self.connected=False
                
    
    def getData(self,port):
        portnum=port
        if len(self.outQueues[portnum])>0:
            return self.outQueues[portnum].popleft()
        else:
            return None
            
    def getMultiStream(self,queryString):
        print "connect multistream %s"%queryString
        try:
            if int(queryString)<self.portCount and int(queryString)>=0:
                return self.dataSenders[int(queryString)]
        except ValueError,v:
            print "bad query string for q sensor collector"
            return None


def main(sensorPort,portBase):
    globalCollector = VilistusSerialCollector()
    globalCollector.run(sensorPort,portBase)

    
if __name__ == '__main__': 


    if len(sys.argv)>=3:
        main(sys.argv[1],int(sys.argv[2]))
    else:
        print ""
        print "usage: QSensorCollector <com port> <Network port>"
        print "      <com port > Com port of q sensor (eg. COM21)"
        print "      <Network port>  Network port for Vicarious stuff to connect to(eg. 49990)"
        print ""
    
