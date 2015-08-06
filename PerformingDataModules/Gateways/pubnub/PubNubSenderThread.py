import threading
import time
import json
from performingdata.Pubnub import Pubnub


class PubNubSenderThread(threading.Thread):

    def __init__(self):
        threading.Thread.__init__(self)
        self.pubnub = Pubnub("pub-c-e655613e-f776-4301-9f29-f71edbcd6559",
                        "sub-c-2eafcf66-c636-11e3-8dcd-02ee2ddab7fe",
                        "sec-c-ZjUwZDgzMTItYzE2Mi00ZGYyLTg2NGMtNmE5N2Q3MGI0MTli",
                        False)
        self.ready = False
        self.message = None
        self.debug = False

    def run(self):
        while True:
            try:
                if self.ready:
                    if self.debug:
                        print "publishing"
                    message = json.loads(self.message)
                    self.pubnub.publish(message)
                    self.ready = False
                    self.message = None
                    if self.debug:
                        print "published"
            except:
                if self.debug:
                    print "Error publishing"
                self.ready = False
                self.message = None       


