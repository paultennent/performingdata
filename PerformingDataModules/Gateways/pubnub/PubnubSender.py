from performingdata.Consumer import Consumer
from performingdata.Pubnub import Pubnub
import json
import sys
import datetime
import calendar


class PubnubSender():

    def __init__(self, host, port, channel, mybuffer):
        self.consumer = Consumer(host, port, self)
        self.pubnub = Pubnub("pub-c-e655613e-f776-4301-9f29-f71edbcd6559",
                        "sub-c-2eafcf66-c636-11e3-8dcd-02ee2ddab7fe",
                        "sec-c-ZjUwZDgzMTItYzE2Mi00ZGYyLTg2NGMtNmE5N2Q3MGI0MTli",
                        False)
        self.buffer = ((mybuffer == "yes") or (mybuffer == 'y'))
        self.channel = channel

    def run(self):
        while(True):
            self.consumer.waitData()
            data = self.consumer.getData()
            if data is not None:
                time = str(calendar.timegm(
                    datetime.datetime.utcnow().utctimetuple()
                ))
                message = {}
                message['channel'] = self.channel
                if not self.buffer:
                    message['message'] = {}
                    message['message'][time] = data
                else:
                    message['message'] = json.loads(data)
                self.pubnub.publish(message)
                # print "publishing"

def main(host, port, channel, mybuffer):
    # running with mybuffer as yes|y expects a json string instead of a single value
    # use the jsonbufferprocessor to build these
    chart = PubnubSender(host, port, channel, mybuffer)
    chart.run()

if __name__ == '__main__':
    main(sys.argv[1], int(sys.argv[2]), str(sys.argv[3]), str(sys.argv[4]))
