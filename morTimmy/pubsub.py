#!/usr/bin/env python

# This library provides a design pattern based on the
# publish-subscribe model.
#
# Further reading:
# http://www.slideshare.net/ishraqabd/publish-subscribe-model-overview-13368808

import asyncio
import json
import queue
import socket


class Publisher:
    """ TODO: Create a interface layer class
    to be able to exchange traffic with the message
    handler. The idea is that we have options for
    serial and socket interfaces """

    def __init__(self, hub_address, hub_port):
        self.protocol = ClientProtocol(hub_address, hub_port)

    def register_channel(self, channel):
        print("Registering channel {}".format(channel))
        message = {"type": "register", "channel": channel}
        self.protocol.send_message(message)

    def notify(self, channel, data):
        print("Sending notification {} to channel {}".format(data, channel))
        message = {"type": "notify", "channel": channel, "data": data}
        self.protocol.send_message(message)


class Subscriber:
    def __init__(self, hub_address, hub_port):
        self.protocol = ClientProtocol(hub_address, hub_port)

    def subscribe(self, channel):
        print("Subscribing to channel {}".format(channel))
        message = {"type": "subscribe", "channel": channel}
        self.protocol.send_message(message)

    def unsubscribe(self, channel):
        print("Unsubscribing from channel {}".format(channel))


class ClientProtocol:
    """ A generic socket class that can be used by both
    Publisher and Subscriber classes

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


class SubscriberClientProtocol(asyncio.Protocol):
    """ Client Protocol for the Subscriber Class """

    def __init__(self, loop):
        self.transport = None
        self.loop = loop
        self.send_queue = asyncio.Queue()
        self.recv_queue = queue.Queue()       # do i need asyncio queues?
        self._ready = asyncio.Event()
        asyncio.async(self._send_messages())  # asyncio.ensure_future >py3.4.3

    @asyncio.coroutine
    def _send_messages(self):
        """ Send messages to the server as they become available.

        The queue gets populated by the send_messages coroutine'
        """
        # Pause this subroutine until self._ready.set() is issued
        yield from self._ready.wait()
        print("Waiting for messages in the queue...")
        while self._ready.is_set():
            # Pause this subroutine until self.send_queue.get() is completed
            message = yield from self.send_queue.get()
            self.transport.write(message.encode())
            print('Message sent: {!r}'.format(message))

    @asyncio.coroutine
    def send_message(self, message):
        """ Feed a message to the _send_messages coroutine.

        This coroutine allows you to feed messages into the
        protocol from outside of the class definition
        """
        # pause this subroutine until self.send_queue.put(message) is completed
        yield from self.send_queue.put(message)

    def connection_made(self, transport):
        """ Upon connection send the message to the
        server

        A message has to have the following items:
            type:       subscribe/unsubscribe
            channel:    the name of the channel
        """
        self.transport = transport
        print("Connected to server")
        self._ready.set()

    def data_received(self, data):
        """ After sending a message we expect a reply
        back from the server

        The return message consist of three fields:
            type:           subscribe/unsubscribe
            channel:        the name of the channel
            channel_count:  the amount of channels subscribed to
        """
        for msg in data.decode().splitlines():
            self.recv_queue.put(msg)

        while not self.recv_queue.empty():
            recv_message = json.loads(self.recv_queue.get())
            print('Message received: {!r}'.format(recv_message))

    def connection_lost(self, exc):
        print('The server closed the connection')


if __name__ == '__main__':
    pass
