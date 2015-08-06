from performingdata.Processor import Processor
import sys
from collections import deque
# processes an ECG stream to get a quick and dirty heart rate out
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

    DIFF_QUEUE_COUNT = 10
    BIG_QUEUE_COUNT =512
    THRESHOLD=2.0
    
    def __init__(self):
        Processor.__init__(self,"ECGHRProcessor", [("source","Raw ECG pulse data")], ["Heart rate (bpm)"],[])
        self.currentRate=0

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
        if bestTimeStep==0 or bestTimeStep==None or bestTimeStepDifference>0.1:
            # if we couldn't find a decent match, return None ie. don't change estimated hr
            #print bestTimeStepDifference
            return None
        #print bestTimeStepDifference,"*"
        return (60.0 / bestTimeStep)
                    
    def variance(self,data):   
        count=0.0
        diffUp=0.0
        lastVal=None
        for c in data:
            if lastVal!=None and lastVal<c:
                diffUp+=c-lastVal
            lastVal=c
            count+=1.0    
        if count==0:
            return 0.0
        return diffUp/count

            
    # main data processing loop - read in a load of data
    def process(self,timeStamp,values,queueNo):
        if self.lasttime==None:
            self.lasttime=timeStamp
        # data point
        curValue = float(values[0])
        self.bigQueue.append(curValue)
        self.diffQueue.append(curValue)
        if len(self.bigQueue)>2:
            varianceBig=self.variance(self.bigQueue)
            varianceSmall=self.variance(self.diffQueue)
#                diffSmall = max(self.diffQueue)-min(self.diffQueue)
#                varianceSmall=diffSmall*diffSmall
#                print varianceBig,varianceSmall
            if varianceBig!=0:
                curTime=timeStamp
            # uncomment these to graph the variance value etc
#                    self.outqueue.append(str(varianceSmall/varianceBig))
#                    self.outqueue.append(str(10.0))
                if varianceSmall>varianceBig*HRProcessor.THRESHOLD and curTime - self.lastBeatTime > 0.25:
#                        print "beat"
                    self.lastBeatTime=curTime
                    self.beatTimes.append(curTime)
                    # calculate a heart rate and output it
                    rate=self.calculateHeartRate()
                    #print rate
                    self.lasttime = curTime
                    if rate!=None:
                        self.addProcessedValues(rate)
        if(timeStamp - self.lasttime > 5.0):
            self.addProcessedValues(0.0)
            self.lasttime=timeStamp
        self.dataCount+=1

                    
if __name__ == '__main__': HRProcessor()
