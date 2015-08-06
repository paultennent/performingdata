from performingdata.Processor import Processor
import sys
from collections import deque

#takes a rolling window average of an incoming signal. Rate is passed as a paremeter along with window size to allow for reduction of data. 

class WindowThresholdCounterProcessor(Processor):

    
    def __init__(self):
        Processor.__init__(self,"WindowThresholdCounterProcessor", [("source","Raw data"),("regularsource","Standard Data Rate Control")], ["Threshold Peak Count"],[("WindowSize","Number of values to calculate threshold count from","1920"),("RollRate","Rate at which the window moves-effectively the output rate","1920"),("Threshold","THreshold Value to count over","90")])
        self.windowCounter = 0
        self.rollCounter = 0
        self.sum = 0
        self.run()
      
    def isThreshold(self,data):    
        isOver = False
        for i in range(0,len(data)):
            if data[i] >= self.threshold:
                isOver = True
        return isOver

    def processArguments(self,firsttimeStamp):
        self.windowSize = int(self.argumentValues[0])
        self.rollRate = int(self.argumentValues[1])
        self.threshold = float(self.argumentValues[2])
        self.histQueue=deque([],self.windowSize)
        
    def process(self,timeStamp,values,queueNo):
        self.histQueue.append(float(values[0]))
        self.rollCounter -= 1
        if len(self.histQueue) < self.windowSize:
            return
        if self.rollCounter <= 0:        
            if self.isThreshold(self.histQueue):
                self.sum = self.sum + 1
            self.rollCounter = self.rollRate
            self.addProcessedValues(str(self.sum))
            return
        
        
                    
if __name__ == '__main__': WindowThresholdCounterProcessor()
