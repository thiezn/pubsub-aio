#!/usr/bin/env python

# This library provides a design pattern based on the
# publish-subscribe model.
#
# Further reading:
# http://www.slideshare.net/ishraqabd/publish-subscribe-model-overview-13368808

import json
import queue
import socket


class PubSubClient:
    """ TODO: Create a interface layer class
    to be able to exchange traffic with the message
    handler. The idea is that we have options for
    serial and socket interfaces """

    def __init__(self, hub_address, hub_port):
        self.protocol = ClientTcpProtocol(hub_address, hub_port)

    def register_channel(self, channel):
        print("Registering channel {}".format(channel))
        message = {"type": "register", "channel": channel}
        self.protocol.send_message(message)

    def notify(self, channel, data):
        print("Sending notification {} to channel {}".format(data, channel))
        message = {"type": "notify", "channel": channel, "data": data}
        self.protocol.send_message(message)

    def subscribe(self, channel):
        print("Subscribing to channel {}".format(channel))
        message = {"type": "subscribe", "channel": channel}
        self.protocol.send_message(message)

    def unsubscribe(self, channel):
        print("Unsubscribing from channel {}".format(channel))


class ClientTcpProtocol:
    """ A generic socket class that can be used by the PubSubClient

    This doesn't use asyncio to keep the clients simple.
    hopefully by having the various clients as completely
    seperate programs we won't need any multiprocessing
    or threading but rather leave it to the OS to handle
    """

    def __init__(self, hub_address, hub_port):
        """ Set up the TCP connection to the pubsub Hub """
        self.recv_queue = queue.Queue()
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((hub_address, hub_port))
        self.sock.setblocking(0)

    def send_message(self, message):
        """ Sends a message to the pubsub Hub

        The type of message should be handled by the Publisher
        and Subscriber classes
        """
        message = (json.dumps(message) + "\r\n").encode('ascii')
        try:
            self.sock.send(message)
        except BlockingIOError:
            # This gets raised when the send queue is full
            print("Send buffer full, can't send message {}".format(message))

    def recv_message(self):
        """ Returns a message received from the pubsub Hub

        It adds any received messages to the recv_queue but
        only returns the first added message from the queue.
        """

        try:
            buff = (self.sock.recv(1024)).decode('ascii')
            self.recv_queue.put(json.loads(buff.strip()))
        except BlockingIOError:
            pass

        if not self.recv_queue.empty():
            return self.recv_queue.get_nowait()
        else:
            return None

    def close(self):
        """ Closes the socket connection """
        self.sock.close()


if __name__ == '__main__':
    pass
