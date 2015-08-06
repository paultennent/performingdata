from performingdata.Consumer import Consumer

_GOODBYE_MESSAGE = u'Goodbye'


def web_socket_do_extra_handshake(request):
    pass


def handleRequest(line, consumer):
    #print "Request:"+line
    data = consumer.getLatest()
    if data is not None:
        return data
    else:
        return "No Data"


def web_socket_transfer_data(request):
    with open("consumer.config") as f:
        content = f.readlines()

    host = content[2].split(":")[1].strip()
    port = int(content[3].split(":")[1].strip())
    print "Connected to:",host, port
    consumer = Consumer(host, port, None)

    while True:
        line = request.ws_stream.receive_message()
        if line is None:
            return
        if isinstance(line, unicode):
            response = handleRequest(line, consumer)
            request.ws_stream.send_message(response, binary=False)
            if line == _GOODBYE_MESSAGE:
                return
        else:
            request.ws_stream.send_message(line, binary=True)
