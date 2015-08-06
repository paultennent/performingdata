from performingdata.ChildDataSender import ChildDataSender
from performingdata.SenderParent import SenderParent
import sys, random, time, math
from threading import Lock


class MultiCollectorSine():
    
    def run(self,port,frequency):
        self.port = port
        self.lastTimes = {}
        self.senders ={}
        self.senderParent=SenderParent(self.port,self)
        self.senderParent.start()        
        self.senderLock=Lock()

        self.curValue = {}

        timeStart = time.clock()
        self.timePerSample = 1.0/float(frequency)
        numSamples=0
        nextTime=timeStart+self.timePerSample
        
        while True:
            curTime=time.clock()
            nextTime+=self.timePerSample
            if curTime<nextTime:
                time.sleep(nextTime-curTime)
            with self.senderLock:
                self.doneSending={}
                for c in self.senders.values():
                    c.dataReady()

    def getMultiStream(self,queryString):
        print queryString
        with self.senderLock:
            if self.senders.has_key(queryString):
                return self.senders[queryString]
            else:
                self.curValue[int(queryString)] = 0
                sender=ChildDataSender(len(self.senders),self)
                self.senders[queryString]=sender
                sender.start()
                return sender

    def getData(self,port):
        curTime=time.clock()
        if not self.lastTimes.has_key(port):
            self.lastTimes[port]=time.clock()
        if curTime - self.lastTimes[port] > self.timePerSample:
                self.lastTimes[port]+=self.timePerSample
                rand = self.getNextVal(port) #random.random() #randint(0,200)-100
                return str(rand)
        return None

    def getNextVal(self, port):
        val = math.sin(math.radians(self.curValue[port]))
        val = (val+1.0)/2.0
        self.curValue[port] += 1
        if self.curValue[port] > 359:
            self.curValue[port] = 0
        return val


def main(port,frequency):
    c = MultiCollectorSine()
    c.run(port,frequency)
    
if __name__ == '__main__': main(int(sys.argv[1]),float(sys.argv[2]))
