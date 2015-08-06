from performingdata.Processor import Processor
import sys
from collections import deque
# processes a breathing stream to get a quick and dirty breathing rate out
# how it does it:
#
# take first derivative
# smooth a lot
# count zero crossings
#
#
# takes the last 8 beat points, and estimates a BPM from that, by 
#a)calculating time deltas between beats
#b)choosing the time delta that is most consistent with the rest of the time deltas
#
# rate is output in beats per minute (float)
        
class BreathProcessor(Processor):

    # 1hz low pass filter (assuming 32hz signal)
    class LowPassFilter():
        def __init__(self):
            self.filterX=[0,0,0,0,0]
            self.filterY=[0,0,0,0,0]
            
        def filterValue(self,value):
            self.filterX.pop()
            self.filterX.insert(0,value/9.716669102e+03)
            self.filterY.pop()
            self.filterY.insert(0,0)
            self.filterY[0] = (  1 * self.filterX[4])\
             + (  4 * self.filterX[3])\
             + ( 6 * self.filterX[2])\
             + (  4 * self.filterX[1])\
             + (  1 * self.filterX[0])\
             + ( -0.5980652616 * self.filterY[4])\
             + (    2.6988843913 * self.filterY[3])\
             + ( -4.5892912321  * self.filterY[2])\
             + (   3.4873077415  * self.filterY[1])
            return self.filterY[0] 

    # filter to find signals between 0.1 and 30 hz (assuming a sample rate of 32hz coming in)
    class BandPassFilter():
        def __init__(self):
            self.filterX=[0,0,0,0,0]
            self.filterY=[0,0,0,0,0]
            
        def filterValue(self,value):
            self.filterX.pop()
            self.filterX.insert(0,value/7.235446167e+00)
            self.filterY.pop()
            self.filterY.insert(0,0)
            self.filterY[0] = (  1 * self.filterX[4])\
             + (  0 * self.filterX[3])\
             + ( -2 * self.filterX[2])\
             + (  0 * self.filterX[1])\
             + (  1 * self.filterX[0])\
             + (-0.2657335233* self.filterY[4])\
             + (   1.2420931517 * self.filterY[3])\
             + ( -2.6718701619 * self.filterY[2])\
             + (  2.6952936676 * self.filterY[1])
            return self.filterY[0] 


    RATE_BEAT_COUNT = 8 # number of breathing beats to calculate rate from - up this to smooth everything out and avoid bad readings, lower it for low latency breathing rate estimates
    
    def __init__(self):
        Processor.__init__(self,"BreathProcessor", [("Breath","Raw respiration stream")], ["Respiration rate (bpm)"],[])    
        self.currentRate=0

        # minimum and maximums - nb. minimum and maximum are reset when an R spike is detected (both to current value, then 
        self.lastValue=None
        self.lastDerivative=None
        self.lastSecondDerivative=0
        self.beatTimes=deque([],BreathProcessor.RATE_BEAT_COUNT)
        self.dataCount=0
        self.lastUnsmoothedValue=0
        self.bandpass=BreathProcessor.BandPassFilter()
        self.lowpass=BreathProcessor.BandPassFilter()
        self.run()

    # process initial arguments
    def processArguments(self,initialTime):
        self.lasttime = initialTime
        self.lastBeatTime=initialTime-10.0

        
    # given a list of detected beat times, get out a breathing rate - this uses multiple beats to avoid dropped / extra beats messing things up
    # on average BPM will go right after RATE_BEAT_COUNT/2 beats
    def calculateBreathingRate(self):
        if len(self.beatTimes)<2:
            return None
        timeSteps=[]
        lastTime=self.beatTimes[0]
        for c in range(1,len(self.beatTimes)):
            timeSteps.append(self.beatTimes[c]-lastTime)
            lastTime=self.beatTimes[c]
#        print timeSteps
        
        # we have a list of beat differences
        # how to find the most consistent bpm?
        
        # assume at least one of the beat differences is right
        # then take each one, and try it, see how many others it is close to (ie.minimum squared difference)
        bestTimeStep=None
        bestTimeStepDifference=None
        for c in timeSteps:
            timeStepDifference=0
            for d in timeSteps:
                timeStepDifference+= (c-d)*(c-d)
            if bestTimeStep==None or timeStepDifference<bestTimeStepDifference:
                bestTimeStep=c
                bestTimeStepDifference=timeStepDifference
        # timesteps is in seconds 
        # so divide 1  by timestep, multiply by  60  to get beats per minute
        if bestTimeStep==0 or bestTimeStep==None:
            return None
        return (60.0 / bestTimeStep)
                    
    # main data processing fn - process one sample
    def process(self,timeStamp,values,queueNo):
            curValue = float(values[0])
            # only do 1 in 8 values - as we get 256hz signal but only really get 32hz
            if self.dataCount&7 == 0:
                if self.lastValue!=None:
                    if self.lastDerivative!=None:
                        derivative=self.lastDerivative*.95 + 0.05*(curValue-self.lastValue)
                    else:
                        derivative=(curValue-self.lastValue)
                    # count zero crossings only
                    if derivative<0.0 and self.lastDerivative>=0.0:
                        if timeStamp - self.lastBeatTime > 0.25:
#                            print "beat"
                            self.lastBeatTime=timeStamp
                            self.beatTimes.append(timeStamp)
                            # calculate a heart rate and output it
                            rate=self.calculateBreathingRate()
                            self.lasttime = timeStamp
                            if rate!=None:
                                self.addProcessedValues(str(rate))
                    self.lastDerivative=derivative
                self.lastValue=curValue
            if(timeStamp - self.lasttime > 20.0):
                self.addProcessedValues(str(0.0))
                self.lasttime=timeStamp-15.0
            self.dataCount+=1
                    
if __name__ == '__main__': BreathProcessor()
