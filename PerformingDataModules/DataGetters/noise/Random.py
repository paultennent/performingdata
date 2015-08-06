from performingdata.DataSender import DataSender
from collections import deque
import sys
import random
import time


class Collector():

    def run(self, port, frequency):
        self.port = port
        self.outqueue = deque([], 1000)
        self.datasender = DataSender(self.port, self)
        self.datasender.start()
        timeStart = time.clock()
        timePerSample = 1.0/float(frequency)
        numSamples = 0
        nextTime = timeStart+timePerSample
        while(True):
            self.setData()
#            print "."
#            print len(self.outqueue)
            numSamples = numSamples + 1
            nextTime += timePerSample
#            print nextTime
            curTime = time.clock()
            if curTime < nextTime:
                time.sleep(nextTime - curTime)
            elif curTime - nextTime > 5.0 or len(self.outqueue) > 100:
                print "Overrun"
#            if curTime-timeStart>5.0 and curTime-timeStart<5.01:
#                print len(self.outqueue)

    def getData(self, port):
        if len(self.outqueue) > 0:
            return self.outqueue.popleft()
        else:
            return None

    def setData(self):
        rand = random.randint(0, 200)
        self.outqueue.append(str(rand))
        self.datasender.dataReady()


def main(port, frequency):
    c = Collector()
    c.run(port, frequency)


if __name__ == '__main__':
    main(int(sys.argv[1]), float(sys.argv[2]))
