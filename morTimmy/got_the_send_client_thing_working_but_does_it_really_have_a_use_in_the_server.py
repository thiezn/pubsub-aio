#!/usr/bin/env python3

import asyncio
import json
import queue  # Should eventually be replaced with asyncio queue maybe?
import logging


class PubHubServerProtocol(asyncio.Protocol):
    """ A Server Protocol listening for subscriber messages """

    def __init__(self, pubsubhub, loop):
        self.loop = loop
        self.pubsubhub = pubsubhub
        self.recv_queue = queue.Queue()
        self.send_queue = asyncio.Queue()
        self._ready = asyncio.Event()
        asyncio.async(self._send_messages())

    def connection_made(self, transport):
        """ Called when connection is initiated """

        self.peername = transport.get_extra_info('peername')
        print("connection from {}".format(self.peername))
        logging.info('connection from {}'.format(self.peername))
        self.transport = transport
        self._ready.set()

    def enc_msg(self, message):
        """ This function takes a dictionary
        and serializes it to a message ready to send
        """
        msg = json.dumps(message, separators=(',', ':')) + "\r\n"
        return msg.encode()

    def data_received(self, data):
        """ The protocol expects a json message containing
        the following fields:

            type:       subscribe/unsubscribe
            channel:    the name of the channel

        Upon receiving a valid message the protocol registers
        the client with the pubsub hub. When succesfully registered
        we return the following json message:

            type:           subscribe/unsubscribe/unknown
            channel:        The channel the subscriber registered to
            channel_count:  the amount of channels registered
        """

        # Receive a message and decode the json output.
        # TODO for some reason I need to do the intermediate tmp variable
        # maybe the data received needs to be clearned every time?

        self.loop.create_task(self.feed_message({'test'}))

        for msg in data.decode().splitlines():
            self.recv_queue.put(msg)

        while not self.recv_queue.empty():
            recv_message = json.loads(self.recv_queue.get())

            # Check the message type and subscribe/unsubscribe
            # to the channel. If the action was succesful inform
            # the client.
            if recv_message['type'] == 'subscribe':
                self.pubsubhub.register_subscriber(self.peername,
                                                   recv_message['channel'])
                send_msg = self.enc_msg(recv_message)
                self.transport.write(send_msg)
            elif recv_message['type'] == 'unsubscribe':
                self.pubsubhub.unregister_subscriber(self.peername,
                                                     recv_message['channel'])
                logging.info('Client {} unsubscribed from {}'
                             .format(self.peername, recv_message['channel']))
                logging.debug('subscribers are now {}'
                              .format(pubsubhub.subscribers))
                send_message = json.dumps({'type': 'unsubscribe',
                                           'channel': recv_message['channel'],
                                           'channel_count': 9},
                                          separators=(',', ':')) + "\r\n"
                self.transport.write(send_message.encode())
            elif recv_message['type'] == 'disconnect':
                logging.info("Closing connection to {}".format(self.peername))
                self.pubsubhub.disconnect_subscriber(self.peername)
                send_message = json.dumps({'type': 'disconnect'},
                                          separators=(',', ':')) + "\r\n"
                self.transport.write(send_message.encode())
                self.transport.close()
            elif recv_message['type'] == 'notify':
                logging.info("client {} published {} to channel {}"
                             .format(self.peername, recv_message['data'],
                                     recv_message['channel']))
                self.pubsubhub.notify_subscribers(self.peername, recv_message)

                for subscribers in self.pubsubhub.subscribers[recv_message['channel']]:



            else:
                logging.warning('Invalid message type {}'
                                .format(recv_message['type']))
                send_message = json.dumps({'type': 'unknown_type'},
                                          separators=(',', ':')) + "\r\n"
                self.transport.write(send_message.encode())
                logging.debug('Sending {!r}'.format(send_message))

    @asyncio.coroutine
    def _send_messages(self):
        """ Send messages to the server when there are any in the queue

        The queue gets populated by the send_messages coroutine'
        """
        # Pause this subroutine until self._ready.set() is issueda
        print("hit _send_message")
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
        print("hit send_message")
        # pause this subroutine until self.send_queue.put(message) is completed
        yield from self.send_queue.put(message)

    @asyncio.coroutine
    def feed_message(self, message):
        """ coroutine to add a message on the send queue

        Use the following code to add a message to the send queue:
          self.loop.create_task(self.feed_message({'test'}))
        """
        print("hit feed_message")
        message = json.dumps({'type': 'subscribe', 'channel': 'sensor'},
                             separators=(',', ':'))
        message += "\r\n"
        # Pause this subroutine until protocol.send_message has a result
        yield from self.send_message(message)

    def eof_received(self):
        """ an EOF has been received from the client.

        This indicates the client has gracefully exited
        the connection. Inform the pubsub hub that the
        subscriber is gone
        """
        logging.info('Client {} closed connection'.format(self.peername))
        self.pubsubhub.disconnect_subscriber(self.peername)
        self.transport.close()

    def connection_lost(self, exc):
        """ A transport error or EOF is seen which
        means the client is disconnected.

        Inform the pubsub hub that the subscriber has
        dissapeared
        """
        if exc:
            logging.warning('{} {}'.format(exc, self.peername))


class PubSubHub:
    def __init__(self, name="MyPubSubHub"):
        """ A hub for all publisher and subscribers

        Publishers and subscribers use the hub as an
        intermediate layer. It registers publishers and
        subscribers. When something gets published the
        hub ensures the message is delivered to all
        subscribers

        The hub requires an underlying transport/protocol
        implementation to provide the low level send and
        receive of messages.

        TODO: provide a way to exchange data between
        multiple PubSubHub's. This would allow for a
        greater distributed architecture.
        """
        self.name = name
        self.subscribers = {}
        self.publishers = {}
        self.notify_queue = []

    def register_subscriber(self, subscriber, channel):
        self.subscribers.setdefault(channel, []).append(subscriber)
        logging.info('Client {} subscribed to {}'.format(subscriber, channel))
        logging.debug('subscribers are now {}'.format(self.subscribers))

    def unregister_subscriber(self, subscriber, channel):
        self.subscribers[channel].remove(subscriber)

    def disconnect_subscriber(self, subscriber):
        for key, value in self.subscribers.items():
            if subscriber in value:
                self.subscribers[key].remove(subscriber)
                logging.info("{} removed from {} because of disconnect message"
                             .format(subscriber, key))

    def notify_subscribers(self, peername, recv_message):
        for subscriber in self.subscribers[recv_message['channel']]:
            self.notify_queue.append({'peer': subscriber,
                                      'channel': recv_message['channel'],
                                      'data': recv_message['data']})

    def register_publisher(self, publisher, channel):
        self.publishers.setdefault(channel, []).append(publisher)

    def unregister_publisher(self, publisher):
        for key, value in self.publishers.items():
            if publisher in value:
                self.publishers[key].remove(publisher)
                logging.info("publisher {} removed".format(publisher))


if __name__ == '__main__':

        logging.basicConfig(filename='log.server',
                            level=logging.DEBUG,
                            filemode='w',
                            format='%(asctime)s %(levelname)s %(message)s')

        logging.info('Initialising the PubSub Hub application')
        pubsubhub = PubSubHub()
        loop = asyncio.get_event_loop()
        # Each client will create a new protocol instance
        coro = loop.create_server(lambda: PubHubServerProtocol(pubsubhub,
                                                               loop),
                                  '127.0.0.1', 10666)
        server = loop.run_until_complete(coro)

        # Serve requests until Ctrl+C
        logging.info('Serving on {}'.format(server.sockets[0].getsockname()))
        print('Serving on {}'.format(server.sockets[0].getsockname()))
        try:
            loop.run_forever()
        except KeyboardInterrupt:
            pass

        # Close the server
        try:
            server.close()
            loop.until_complete(server.wait_closed())
            loop.close()
        except:
            pass
