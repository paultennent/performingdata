from performingdata.ChildDataSender import ChildDataSender
from performingdata.SenderParent import SenderParent
import sys, random,time
from threading import Lock


class BrownNoiseMultiCollector():
    
    def run(self,port,frequency):
        self.port = port
        self.lastTimes = {}
        self.senders ={}
        self.senderParent=SenderParent(self.port,self)
        self.senderParent.start()        
        self.senderLock=Lock()
        self.out = {}
        
        filename = "noisecollectorout.csv"
        try:
            self.f = open(filename, 'w')
            self.f.write("Happy,Angry,Fear,Surprise,Sad,Disgust\n")
            print "written to file"
        except:
            print "Error opening", filename
        

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
                    
    def brownNoise(self,val):
        q = random.random()
        if q > 0.5:
            val = val + ((1-q)/5)
            if val > 1.0:
                return val - (2*((1-q)/10))
        else:
            val = val - ((q)/5)
            if val < 0.0:
                return val + (2*((q)/10))
        return val
            
    
    def getMultiStream(self,queryString):
        print queryString
        with self.senderLock:
            if self.senders.has_key(queryString):
                return self.senders[queryString]
            else:
                sender=ChildDataSender(len(self.senders),self)
                self.senders[queryString]=sender
                self.out[queryString] = 0.5
                sender.start()
                return sender
            
    def getData(self,port):
        curTime=time.clock()
        if not self.lastTimes.has_key(port):
            self.lastTimes[port]=time.clock()
        if curTime - self.lastTimes[port] > self.timePerSample:
            self.lastTimes[port]+=self.timePerSample
            if len(self.out) > 0:
                self.out[str(port)] = self.brownNoise(self.out[str(port)])
                if port == 5:
                    for i in range(0,len(self.out)):
                        self.f.write(str(self.out[str(i)]))
                        if i < len(self.out)-1:
                            self.f.write(",")
                    self.f.write("\n")
                return str(self.out[str(port)])
        return None
        
                
        
def main(port,frequency):
    c = BrownNoiseMultiCollector()
    c.run(port,frequency)
    
if __name__ == '__main__': main(int(sys.argv[1]),int(sys.argv[2]))
