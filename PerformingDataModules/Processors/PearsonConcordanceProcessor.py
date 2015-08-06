from performingdata.Processor import Processor
import sys
from collections import deque
from datetime import *
from scipy import stats as stats
from numpy import *
import time


class PearsonConcordanceProcessor(Processor):

    def __init__(self):
        Processor.__init__(self,"PearsonConcordanceProcessor", [("Signal1","Signal 1 Stream"),("Signal2","Signal 2 Stream")], ["P value for the two streams", "Correlation value for the two streams"],[("Window Size","Number of input values per output value","15")])      
        self.windowSize=256
        self.dominantQ = None
        self.bothQsActive = False
        self.run()
        

    # process initial arguments
    def processArguments(self,initialTime):
        self.windowSize=int(self.argumentValues[0])       
        self.histQueue=deque([],self.windowSize)
        self.histQueue2=deque([],self.windowSize)    
        
        
    # main data processing function
    def process(self,timeStamp,values,queueNo):
        if self.dominantQ == None:
            self.dominantQ = queueNo
        if queueNo != self.dominantQ:
            self.bothQsActive = True

        if self.bothQsActive:
            if self.dominantQ == queueNo:
                
                self.histQueue.append(float(values[0]))
                self.histQueue2.append(float(values[1]))
                if len(self.histQueue) < self.windowSize:
                    return
                
                self.vals1 = array(self.histQueue)
                self.vals2 = array(self.histQueue2)
                (result,presult) = stats.pearsonr(self.vals1, self.vals2)
                self.addProcessedValues([presult,result])
            
                    
if __name__ == '__main__': PearsonConcordanceProcessor()
