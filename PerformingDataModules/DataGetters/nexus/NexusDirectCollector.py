
#get data from a nexus biofeedback box - needs biotrace running etc. JM:beware - I just hacked this up for testing, no guarantees!

#from ChildDataSender import ChildDataSender
import serial
from collections import deque
import sys, random,time
from performingdata.SenderParent import *
import struct

class NexusDirectCollector():
    
    def run(self,nexusComPort,port):
        self.portBase=port
        self.comport=nexusComPort
        while True:
            try:
                self.port = serial.Serial(self.comport,115200,timeout=10)

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
           
                #    start reading data from nexus
                self.writePort("AA AA 00 05 56 50")
                  
                  
                #port setup
                self.writePort("AA AA 02 22 00 00 80 00 D4 32")
                self.writePort("AA AA 02 22 80 00 53 00 81 32")
                self.writePort("AA AA 02 22 00 10 80 00 D4 22")
                self.writePort("AA AA 02 22 01 10 80 00 D3 22 	")
                self.writePort("AA AA 02 22 00 00 80 00 D4 32 ")
                self.writePort("AA AA 02 22 80 00 53 00 81 32 ")
                self.writePort("AA AA 02 22 00 10 80 00 D4 22 ")
                self.writePort("AA AA 02 22 01 10 80 00 D3 22 	")

                #go
                self.writePort("AA AA 03 02 0E 00 00 00 00 00 45 53")

                #stay alive
                self.writePort("AA AA 00 27 56 2E",False)

                singlePacket=[]
                foundPacket=0
                
                self.despikes=[0,0,0]



                stayalivelast=time.clock()        
                while True:
                    #    stay alive
                    if (time.clock()-stayalivelast)>0.5:
                        self.writePort("AA AA 00 27 56 2E",False)
                        stayalivelast=time.clock()                
                        #    print "Stay alive"
                        
                    data=self.port.read(64)
                    packetBuffer=map(ord,data)
                    for c in packetBuffer:
                        if c==0xaa:
                            foundPacket+=1
                        else:
                            if foundPacket>=2:
                                # dump packet
        #                        for val in singlePacket:
        #                            print "%02x"%val,
        #                        print ""
                                self.sendPacketValues(singlePacket)
                                singlePacket=[]
                            foundPacket=0
                        singlePacket.append(c)
            except serial.SerialException,e:
                print "Couldn't connect Nexus on ",nexusComPort
                time.sleep(5)
        
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
        
    tempCounter=0    
        
    def sendPacketValues(self,packet):
        if len(packet)>=40:
            # print out 10 raw channels
            data=[0,0,0,0,0,0,0,0,0,0,0]
            NexusDirectCollector.tempCounter+=1
            for c in range(0,10):
                data[c] = packet[c*3+2] + (packet[c*3+3]<<8) + (packet[c*3+4]<<16)
                if data[c]>=8388608:
                    data[c]-=16777216
                if NexusDirectCollector.tempCounter<30:
                    print "%06d"%(data[c]>>8),
            if NexusDirectCollector.tempCounter<30:
                print ""
            self.despikeGSR(data)
            for (c,ds) in enumerate(self.dataSenders):
                if ds.isConnected():
                    self.outQueues[c].append("%f"%data[c])
#                    print "%06d"%(data[c]>>8),"%f"%data[c]
            # trigger the data senders all at once
            for ds in self.dataSenders:
                if ds.isConnected():
                    ds.dataReady()

    def makeStringFromHex(self,hexString):
        str=""
        for c in hexString.split(" "):
            try:
                num=int("0x%s"%c,16)
                str+=chr(num)
            except ValueError:
                None
        return str
                    
    def writePort(self,hexString,display=True):
        realString=self.makeStringFromHex(hexString)
        if display:
            print "Write: %d"%len(realString),hexString
        self.port.write(realString)

                    
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
        
def main(comport,portBase):
    c = NexusDirectCollector()
    c.run(comport,portBase)
    
if __name__ == '__main__': 
    if len(sys.argv)==3:
        main(sys.argv[1],int(sys.argv[2]))
    else:
        print ""
        print "usage: NexusDirectCollector <COM port> <Network port>"
        print "      <COM port>      Nexus Bluetooth COM Port (eg. COM1)"
        print "      <Network port>  Network port for Vicarious stuff to connect to (eg. 49990)"
        print ""
      
