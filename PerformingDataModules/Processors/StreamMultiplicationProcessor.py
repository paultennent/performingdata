from performingdata.Processor import Processor
import sys
from collections import deque
import time


class StreamMultiplicationProcessor(Processor):

    def __init__(self):
        Processor.__init__(self,"StreamMultiplicationProcessor", [("Signal1","Signal 1 Stream"),("Signal2","Signal 2 Stream")], [("Arousal","Multiplied value of the two streams")],[("divideTotal","divide the total by this value","100")])      
        self.dominantQ = None
        self.run()  

    # process initial arguments
    def processArguments(self,initialTime):
        self.divideTotal = float(self.argumentValues[0])     
        
    # main data processing function
    def process(self,timeStamp,values,queueNo):
        if self.dominantQ == None:
            self.dominantQ = queueNo
        elif self.dominantQ == queueNo:
            
            val = (float(values[0]) * float(values[1])/self.divideTotal)
            self.addProcessedValues(val)
            
                    
if __name__ == '__main__': StreamMultiplicationProcessor()
