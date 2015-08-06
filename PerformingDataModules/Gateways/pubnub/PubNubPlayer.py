from performingdata.Pubnub import Pubnub
import sys
import random
import time
import json
from PubNubSenderThread import PubNubSenderThread


class PubNubPlayer():

    def run(self, channel, frequency, filename):

        self.channel = channel
        self.filename = filename
        self.file = open(filename, "r")
        self.debug = True

        self.pubnub = PubNubSenderThread()
        self.pubnub.start()

        self.channel = channel
        timeStart = time.clock()
        timePerSample = 1.0/float(frequency)
        numSamples = 0
        nextTime = timeStart
        while(True):
            self.setData()
            numSamples = numSamples + 1
            nextTime += timePerSample
            curTime = time.clock()
            if curTime < nextTime:
                time.sleep(nextTime - curTime)


    def setData(self):
        message = {}
        message['channel'] = self.channel
        message['message'] = self.fixTimes(json.loads(self.readNextLine()))
        self.pubnub.message = json.dumps(message)
        self.pubnub.ready = True
        if self.debug:
            print "published"

    def fixTimes(self,message):
        timenow = time.time();
        firstTime = float(message['data'][0][0])
        diff = timenow-firstTime
        for i in range(len(message['data'])):
            oldTime = float(message['data'][i][0])
            newtime = oldTime + diff
            # print "oldtime",oldTime,"newtime",newtime, "diff",diff
            message['data'][i][0] = newtime
        return message


    def readNextLine(self):
        data = self.file.readline()
        if data == "":
            self.file = open(self.filename, "r")
            data = self.file.readline()
        return data



def main(channel, frequency, filename):
    c = PubNubPlayer()
    c.run(channel, frequency, filename)


if __name__ == '__main__':
    main(sys.argv[1], float(sys.argv[2]), sys.argv[3])