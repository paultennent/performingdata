from performingdata.Processor import Processor
import sys
from collections import deque

#takes a rolling window average of an incoming signal. Rate is passed as a paremeter along with window size to allow for reduction of data. 

class AveragingWindowProcessor(Processor):

    
    def __init__(self):
        Processor.__init__(self,"AveragingWindowProcessor", [("source","Raw data")], ["Averaged Value"],[("WindowSize","Number of values to calculate average from","640"),("RollRate","Rate at which the window moves-effectively the output rate","128")])
        self.windowCounter = 0
        self.rollCounter = 0
        self.run()
      
    def average(self,data):    
        sum = 0.0
        #Calc sum
        for i in range(0,len(data)):
            sum += data[i]
	
        #Calc ave
        ave = sum/len(data)  
        return ave

    def processArguments(self,firsttimeStamp):
        self.windowSize = int(self.argumentValues[0])
        self.rollRate = int(self.argumentValues[1])
        self.histQueue=deque([],self.windowSize)
        
    def process(self,timeStamp,values,queueNo):
        self.histQueue.append(float(values[0]))
        self.rollCounter -= 1
        if len(self.histQueue) < self.windowSize:
            return
        if self.rollCounter <= 0:        
            avg = self.average(self.histQueue)
            self.rollCounter = self.rollRate
            self.addProcessedValues(str(round(avg,3)))
            return
        
        
                    
if __name__ == '__main__': AveragingWindowProcessor()
