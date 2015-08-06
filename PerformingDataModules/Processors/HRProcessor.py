from performingdata.Processor import Processor
from collections import deque
# processes a BVP stream to get a quick and dirty heart rate out
# how it does it:
#
# two variances - one of a big range (2 seconds), 1 of 10 samples
# if the last 10 samples has much higher (10 times as much) variance, then it assumes this is a beat spike
#
# takes the last 8 beat counts, and estimates a BPM from that, by 
#a)calculating time deltas between beats
#b)choosing the time delta that is most consistent with the rest of the time deltas
#
# rate is output in beats per minute (float)

class HRProcessor(Processor):

    RATE_BEAT_COUNT = 9 # number of heart beats to calculate rate from - up this to smooth everything out and avoid bad readings, lower it for low latency hr estimates

    DIFF_QUEUE_COUNT = 64
    BIG_QUEUE_COUNT =512
    THRESHOLD=2.0
    
    def __init__(self):
        Processor.__init__(self,"HRProcessor", [("source","Raw pulse data")], [("hr","Heart rate (bpm)")],[])
        
        self.currentRate=0

        self.rate=None
        # minimum and maximums - nb. minimum and maximum are reset when an R spike is detected (both to current value, then 
        self.minDerivative=0.0
        self.maxDerivative=0.0
        self.lastValue=None
        self.lastDerivative=None
        self.lastSmoothedDerivative=None
        self.dataCount=0
        self.minDerivativeTime=0
        self.maxDerivativeTime=0
        self.lastBeatTime=-9990
        self.beatTimes=deque([],HRProcessor.RATE_BEAT_COUNT)
        self.diffQueue=deque([],HRProcessor.DIFF_QUEUE_COUNT)
        self.bigQueue=deque([],HRProcessor.BIG_QUEUE_COUNT)
        self.lasttime = None
        self.run()


    # given a list of detected beat times, get out a heart rate - this uses multiple beats to avoid dropped / extra beats messing things up
    # on average BPM will go right after RATE_BEAT_COUNT/2 beats
    def calculateHeartRate(self):

        # minimum of 7 beats before we do anything
        if len(self.beatTimes)<7:
            return None
            
        bestTimeStep=None
        bestTimeStepDifference=None
        # assume that there is possibly at least 1 beat extra
        for ignoredBeat in range(-1,len(self.beatTimes)):            
            timeSteps=[]
            lastTime=None
            for c in range(0,len(self.beatTimes)):                    
                if c!=ignoredBeat:
                    if lastTime!=None:
                        timeSteps.append(self.beatTimes[c]-lastTime)
                    lastTime=self.beatTimes[c]

            # assume at least one of the beat differences is right
            # then take each one, and try it, see how many others it is close to (ie.minimum squared difference)
            for c in timeSteps:
                timeStepDifference=0
                for d in timeSteps:
                    timeStepDifference+= (c-d)*(c-d)
                timeStepDifference/=len(timeSteps)
                if bestTimeStep==None or timeStepDifference<bestTimeStepDifference:
                    bestTimeStep=c
                    bestTimeStepDifference=timeStepDifference

                  
        # timesteps is in seconds per beat
        # so 60 / timestep = bpm
        if bestTimeStep==0 or bestTimeStep==None or bestTimeStepDifference>0.15:
            # if we couldn't find a decent match, return None ie. don't change estimated hr
            #print bestTimeStepDifference
            return None
        #print bestTimeStepDifference,"*"
        return (60.0 / bestTimeStep)
                    
    def varianceUpDown(self,data,startPos=0,endPos=-1):   
        count=0.0
        diffUpDown=0.0
        lastVal=None
        if endPos==-1 or endPos>len(data):
            endPos=len(data)
        for i in range(startPos,endPos):
            c=data[i]
            if lastVal!=None:
                diffUpDown+=abs(c-lastVal)
            lastVal=c
            count+=1.0    
        if count==0:
            return 0.0
        return diffUpDown/count

    def varianceUp(self,data,startPos=0,endPos=-1):   
        count=0.0
        diffUp=0.0
        lastVal=None
        if endPos==-1 or endPos>len(data):
            endPos=len(data)
        for i in range(startPos,endPos):
            c=data[i]
            if lastVal!=None and lastVal<c:
                diffUp+=c-lastVal
            lastVal=c
            count+=1.0    
        if count==0:
            return 0.0
        return diffUp/count

    def varianceDown(self,data,startPos=0,endPos=-1):   
        count=0.0
        diffDown=0.0
        lastVal=None
        if endPos==-1 or endPos>len(data):
            endPos=len(data)
        for i in range(startPos,endPos):
            c=data[i]
            if lastVal!=None and lastVal>c:
                diffDown+=lastVal-c
            lastVal=c
            count+=1.0    
        if count==0:
            return 0.0
        return diffDown/count
            
    # main data processing function
    def process(self,timeStamp,values,queueNo):
        if self.lasttime==None:
            self.lasttime=timeStamp
        # data point
        curValue = float(values[0])
        self.bigQueue.append(curValue)
        self.diffQueue.append(curValue)
        if len(self.bigQueue)>2:
            varianceBig=self.varianceUpDown(self.bigQueue)
            varianceSmall=self.varianceUp(self.diffQueue,0,HRProcessor.DIFF_QUEUE_COUNT/2)+self.varianceDown(self.diffQueue,HRProcessor.DIFF_QUEUE_COUNT/2)
            if varianceBig!=0:
                if varianceSmall>0.5 and varianceSmall>varianceBig*HRProcessor.THRESHOLD and timeStamp - self.lastBeatTime > 0.25:
                    #print "beat"
                    self.lastBeatTime=timeStamp
                    self.beatTimes.append(timeStamp)
                    # calculate a heart rate and output it
                    newRate=self.calculateHeartRate()
                    if self.rate==None:
                        self.rate=newRate
                    elif newRate!=None:
                        self.rate=newRate*0.5+self.rate*0.5
                    self.lasttime = timeStamp
                    if self.rate!=None:
                        self.addProcessedValues(self.rate)
        if(timeStamp - self.lasttime > 5.0):
            self.addProcessedValues(0.0)
            self.lasttime=timeStamp
        self.dataCount+=1

if __name__ == '__main__': HRProcessor()
