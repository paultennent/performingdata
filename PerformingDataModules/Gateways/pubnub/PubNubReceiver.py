from performingdata.ChildDataSender import ChildDataSender
from performingdata.SenderParent import SenderParent
from PubNubReceiverDeliveryThread import PubNubReceiverDeliveryThread
from collections import deque
from performingdata.Pubnub import Pubnub
from json import JSONEncoder
from threading import Lock
import sys
import time


class PubnubCollector():

    def run(self, port, channel, latency, bufferSize):
        self.mutex = Lock()
        self.port = port
        self.outqueuebuffers = {}
        self.outbuffers = {}
        self.senders = {}
        self.channels = None
        self.senderParent=SenderParent(self.port,self)
        self.channel = channel
        self.buffer_size = bufferSize
        self.timestamp_buffer = [None] * self.buffer_size
        self.debug = False

        self.index = 0;
        self.offset = None
        self.latency = latency
        self.setupDone = False
        self.channel_count = 0

        pubnub = Pubnub("pub-c-e655613e-f776-4301-9f29-f71edbcd6559",
                        "sub-c-2eafcf66-c636-11e3-8dcd-02ee2ddab7fe",
                        "sec-c-ZjUwZDgzMTItYzE2Mi00ZGYyLTg2NGMtNmE5N2Q3MGI0MTli",
                        False)

        

        while(True):
            
            pubnub.subscribe({
                'channel': self.channel,
                'callback': self.setData
                })

    def getData(self, port):
        if len(self.outqueuebuffers[port]) > 0:
            return self.outqueuebuffers[port].popleft()
        else:
            return None

    def setData(self, message):
        # print "got data"
        #first time round - can't start senders till we establish the channels
        if self.offset is None:
            self.offset = (time.time() - message["data"][0][0]) + self.latency
            self.channel_count = len(message["channelnames"])
            self.channels = message["channelnames"]
            self.channeltypes = message["channeltypes"]
            for i in range(self.channel_count):
                self.outbuffers[i] = [None] * self.buffer_size
                self.outqueuebuffers[i] = deque([], 1000)
                self.senders[i] = ChildDataSender(i, self)
                self.senders[i].start()
            if self.debug:
                print "Got Data:", len(self.outbuffers)            
            self.senderParent.start()
            # print "finished initial setup"
        self.mutex.acquire()

        for i in range(len(message["data"])):

            self.timestamp_buffer[self.index] = message["data"][i][0]
            vals = message["data"][i][1]

            for j in range(len(vals)):
                self.outbuffers[j][self.index] = vals[j]

            self.index += 1
            if self.index >= self.buffer_size:
                self.index = 0

        self.mutex.release()

        if not self.setupDone:
            self.delThread = PubNubReceiverDeliveryThread(self)
            self.delThread.start()
            self.setupDone = True
            # print message
            # print "started thread"


        return True


    def getMultiStream(self,queryString):
        if self.senders.has_key(int(queryString)):
            return self.senders[int(queryString)]
        return None


def main(port,channel,latency,bufferSize):
    c = PubnubCollector()
    c.run(port,channel,latency,bufferSize)


if __name__ == '__main__':
    # if second argument is no (not buffered) trivial decoding will be used
    # if buffering is to be used, use jsondecodingproicessor instead
    main(int(sys.argv[1]),sys.argv[2],float(sys.argv[3]),int(sys.argv[4]))
