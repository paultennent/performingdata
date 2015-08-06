import threading
import time
from threading import Lock

class PubNubReceiverDeliveryThread(threading.Thread):

    def __init__(self, parent):
        threading.Thread.__init__(self)
        self.parent = parent

    def run(self):
        while True:
            now = time.time()
            due = now - self.parent.offset
            search_from = self.parent.index - 1
            if search_from == - 1:
                search_from = self.parent.buffer_size - 1

            # print "search from", search_from,  "index",self.parent.index, "bufferSize",self.parent.buffer_size
            timestamp = 0
            nextTimestamp = 0
            self.parent.mutex.acquire()

            lastjvalue = 0

            for theid in range(self.parent.channel_count):

                data = self.parent.outbuffers[int(theid)][self.parent.index]
                
                for i in range(search_from, search_from - self.parent.buffer_size, -1):
                    j = i if i >= 0 else i + self.parent.buffer_size
                    #if self.parent.index > 0:
                    #   print "i",i,"j",j,"searchFrom",search_from, "index",self.parent.index
                    timestamp = self.parent.timestamp_buffer[j]
                    if timestamp is not None and timestamp <= due:
                        data = self.parent.outbuffers[int(theid)][j]
                        lastjvalue = j
                        if(j<self.parent.buffer_size-2):
                            nextTimestamp = self.parent.timestamp_buffer[j+1]
                        else:
                            nextTimestamp = self.parent.timestamp_buffer[0]
                        break
                if data is not None:
                # print data
                    self.parent.outqueuebuffers[int(theid)].append(str(data))
                    self.parent.senders[int(theid)].dataReady()
                
            
            # wait for the next timestamp
            if timestamp is not None and nextTimestamp is not None:
                waitTime = nextTimestamp - timestamp
                if waitTime > 0:
                    # print "timestamp", timestamp-1421300000, "nextTimestamp", nextTimestamp-1421300000, "index", self.parent.index, "due", due, "lastjvalue", lastjvalue
                    # print "==="
                    #print "waiting", waitTime
                    time.sleep(waitTime)
            else:
                pass
                # print "timestamp", timestamp, "nextTimestamp", nextTimestamp, "index", self.parent.index, "due", due, "lastjvalue", lastjvalue
                # print "====================================================================================="

            self.parent.mutex.release()