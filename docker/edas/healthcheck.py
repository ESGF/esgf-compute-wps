#! /usr/bin/env python

import sys
import zmq

try:
    context = zmq.Context.instance()

    socket = context.socket(zmq.REQ)

    socket.connect('tcp://0.0.0.0:5670')

    socket.send('0!getCapabilities!WPS')

    data = socket.recv()
except Exception as e:
    print e.message

    socket.close()

    sys.exit(1)
else:
    print data

    socket.close()

    sys.exit(0)
