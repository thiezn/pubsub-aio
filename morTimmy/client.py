#!/usr/bin/env python3

import asyncio
import json
import queue
import pubsub


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
        print('Stop the event loop')
        self.transport.close()
        self.loop.stop()


@asyncio.coroutine
def feed_messages(protocol):
    """ Example function that sends the same message repeadedly. """
    message = json.dumps({'type': 'subscribe', 'channel': 'sensor'},
                         separators=(',', ':'))
    message += "\r\n"
    # Pause this subroutine until protocol.send_message has a result
    yield from protocol.send_message(message)

    # Pause this subroutine until protocol.send_message has a result
    message = json.dumps({'type': 'unsubscribe', 'channel': 'sensor'},
                         separators=(',', ':'))
    message += "\r\n"
    # Pause this subroutine until protocol.send_message has a result
    yield from protocol.send_message(message)

    message = json.dumps({'type': 'subscribe', 'channel': 'blert'},
                         separators=(',', ':'))
    message += "\r\n"
    # Pause this subroutine until protocol.send_message has a result
    yield from protocol.send_message(message)

    """
    message = json.dumps({'type': 'unsubscribe', 'channel': 'blert'},
                         separators=(',', ':'))
    message += "\r\n"
    # Pause this subroutine until protocol.send_message has a result
    yield from protocol.send_message(message)
    """
    message = json.dumps({'type': 'disconnect'},
                         separators=(',', ':'))
    message += "\r\n"
    # Pause this subroutine until protocol.send_message has a result
    yield from protocol.send_message(message)


if __name__ == '__main__':
    subscriber = Subscriber('127.0.0.1', 10666)

    loop = asyncio.get_event_loop()
    coro = loop.create_connection(lambda: SubscriberClientProtocol(loop),
                                  '127.0.0.1', 10666)
    _, proto = loop.run_until_complete(coro)
    task = asyncio.async(feed_messages(proto))  # asyncio.ensure_future >3.4.3

    try:
        loop.run_forever()
    except KeyboardInterrupt:
        print('Closing connection')
    loop.close()
