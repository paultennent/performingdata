

from performingdata.Processor import Processor
import sys
from collections import deque
import time

# processes GSR, Heart Rate to get a 'threat level' from 0 to 3
class MultiplicationProcessor(Processor):

    def __init__(self):
        Processor.__init__(self,"MultiplicationProcessor", [("Input","Input Stream")], ["Multiplied Input"],[("Factor","Value to multiply by","100")])    
        self.run()  

    # process initial arguments
    def processArguments(self,initialTime):
        # make sure that the smoothing value is a float
        self.val=float(self.argumentValues[0])
            
    # main data processing function
    def process(self,timeStamp,values,queueNo):
            out=0
            victim=float(values[0])
            out = victim * self.val
            self.addProcessedValues(out)
            
                    
if __name__ == '__main__': MultiplicationProcessor()
