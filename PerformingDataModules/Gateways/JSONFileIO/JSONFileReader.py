from performingdata.ChildDataSender import ChildDataSender
from performingdata.SenderParent import SenderParent
from collections import deque
import json
import time
import sys


class JSONFileReader():

    def run(self, port, filename):
        self.port = port
        self.outqueuebuffers = {}
        self.outqueues = {}
        self.senders = {}
        self.channels = None
        self.senderParent = SenderParent(self.port, self)

        with open(filename) as f:
            content = f.readlines()

        counter = 0
        while(True):
            if counter >= len(content):
                counter = 0
            message = json.loads(content[counter][:-1])
            self.setData(message)
            counter += 1

    def getData(self, port):
        if len(self.outqueues[port]) > 0:
            return self.outqueues[port].popleft()
        else:
            return None

    def setData(self, message):
        # first time round - can't start senders till we establish the channels
        if not self.channels:
            self.channels = message["channelnames"]
            self.channeltypes = message["channeltypes"]
            for i in range(0, len(message["channelnames"])):
                self.outqueues[i] = deque([], 10000)
                self.senders[i] = ChildDataSender(i, self)
                self.senders[i].start()

            self.senderParent.start()

        # print "data length is", len(message["data"])
        for i in range(0, len(message["data"])):
            thisTime = message["data"][i][0]
            vals = message["data"][i][1]

            for j in range(0, len(self.channels)):
                self.outqueues[j].append(str(vals[j]))
                self.senders[j].dataReady()
            if i < len(message["data"])-2:
                nextTime = message["data"][i+1][0]
                time.sleep(nextTime-thisTime)
            else:
                break

    def getMultiStream(self, queryString):
        if self.senders.has_key(int(queryString)):
            return self.senders[int(queryString)]
        return None


def main(port, filename):
    c = JSONFileReader()
    c.run(port, filename)


if __name__ == '__main__':
    main(int(sys.argv[1]), sys.argv[2])
