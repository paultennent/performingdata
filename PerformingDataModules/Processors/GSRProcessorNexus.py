from performingdata.Processor import Processor
import sys
from collections import deque
# processes a gsr stream to get a rate of change out - resampled to a rolling threshold or something?
# how it does it:
# 1: smooth it 
# 2: Take the moving average of the last n values
#
#

class GSRProcessorNexus(Processor):

    HIST_QUEUE_COUNT = 8
    
    def __init__(self):
        Processor.__init__(self,"GSRProcessorNexus", [("source","Raw gsr data")], ["GSR rate of change"],[("OutputRatio","8")])
        self.currentRate=0

        # minimum and maximums - nb. minimum and maximum are reset when an R spike is detected (both to current value, then 
        self.diffValue=0
        self.lastValue=None
        self.skipValueCount=0
        self.despikeValues=[]
	
        # queue to store last n values for moving average
        self.histQueue=deque([],GSRProcessorNexus.HIST_QUEUE_COUNT)
        self.run()
        
    def processArguments(self,firsttimeStamp):
        self.outputRatio = int(self.argumentValues[0])
        print "output ratio",self.outputRatio
      
    def average(self,data):
        sum = 0.0
        #Calc sum
        for i in range(0,len(data)):
            sum += data[i]
	
        #Calc ave
        ave = sum/len(data)  
        return ave
      
    # removes spikes from nexus gsr data (which is lovely and smooth but has horrible spikes)
    # get first:
    # a b c: out b
    # b c d: out c
    # c d s : out d
    # d s e : out d - delete s
    # d e f : out e
    def deSpikeFilter(self,value):
        self.despikeValues.append(value)
        if len(self.despikeValues)!=4:
            return value
        self.despikeValues.pop(0)
        if self.despikeValues[2]>=self.despikeValues[1]>=self.despikeValues[0]:
            None
        elif self.despikeValues[2]<=self.despikeValues[1]<=self.despikeValues[0]:
            None
        else:
            self.despikeValues[1]=self.despikeValues[0]
        return self.despikeValues[1]
      
      
    # main data processing loop - read in a load of data
    def process(self,timeStamp,values,queueNo):
        self.skipValueCount+=1
        # skip 7 in 8 values as GSR comes in at 256 HZ but only changes at 32 HZ (vilistus joy)
        if self.skipValueCount<self.outputRatio:
            return
            self.skipValueCount=0
            # data point
        curValue = float(values[0])
        curValue=self.deSpikeFilter(curValue)
      
        if self.lastValue!=None:
            # smooth the value
            curValue=self.lastValue*0.9 + curValue*0.1
            
            #Get the difference and the rate of change
            self.diffValue = curValue-self.lastValue
            roc = self.diffValue/0.03125
            
            #Put it in the queue and get the mean value
            self.histQueue.append(roc)		
            aveValue = self.average(self.histQueue)
            self.addProcessedValues(str(round(-0.001*aveValue,3)))
        self.lastValue=curValue
            
                    
if __name__ == '__main__': GSRProcessorNexus()
