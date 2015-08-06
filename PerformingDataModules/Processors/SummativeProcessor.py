from performingdata.Processor import Processor
import sys
from collections import deque
import time


class SummativeProcessor(Processor):
    
    def __init__(self):
        Processor.__init__(self,"SummativeProcessor", [("*sources","List of sources to sum")], ["Sum of all sources"],[])
        self.run()

    def process(self,timestamp,data,queueNo):
        sum=0.0
        for c in data:
            sum+=float(c)
        self.addProcessedValues(sum)
                    
if __name__ == '__main__': SummativeProcessor()
