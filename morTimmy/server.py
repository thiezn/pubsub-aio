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

    def connection_made(self, transport):
        """ Called when connection is initiated """

        self.peername = transport.get_extra_info('peername')
        logging.info('connection from {}'.format(self.peername))
        print('connection from {}'.format(self.peername))
        self.transport = transport

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

        for msg in data.decode().splitlines():
            self.recv_queue.put(msg)

        while not self.recv_queue.empty():
            recv_message = json.loads(self.recv_queue.get())

            # Check the message type and subscribe/unsubscribe
            # to the channel. If the action was succesful inform
            # the client.
            if recv_message['type'] == 'subscribe':
                self.pubsubhub.register_subscriber(self.peername, recv_message['channel'])
                print('Client {} subscribed to {}'.format(self.peername,
                                                          recv_message['channel']))
                print('subscribers are now {}'.format(pubsubhub.subscribers))
                send_message = json.dumps({'type': 'subscribe',
                                           'channel': recv_message['channel'],
                                           'channel_count': 10},
                                          separators=(',', ':'))
            elif recv_message['type'] == 'unsubscribe':
                print(recv_message)
                self.pubsubhub.unregister_subscriber(self.peername, recv_message['channel'])
                print('Client {} unsubscribed from {}'
                      .format(self.peername, recv_message['channel']))
                print('subscribers are now {}'.format(pubsubhub.subscribers))
                send_message = json.dumps({'type': 'unsubscribe',
                                           'channel': recv_message['channel'],
                                           'channel_count': 9},
                                          separators=(',', ':'))
            elif recv_message['type'] == 'disconnect':
                print(recv_message)
                print("Closing connection to {}".format(self.peername))
                send_message = json.dumps({'type': 'disconnect'},
                                          separators=(',', ':'))
            else:
                print('Invalid message type {}'.format(recv_message['type']))
                send_message = json.dumps({'type': 'unknown_type'},
                                          separators=(',', ':'))

            send_message += "\r\n"
            print('Sending {!r}'.format(send_message))
            self.transport.write(send_message.encode())

            if recv_message['type'] == 'disconnect':
                self.transport.close()

    def eof_received(self):
        """ an EOF has been received from the client.

        This indicates the client has gracefully exited
        the connection. Inform the pubsub hub that the
        subscriber is gone
        """
        print('Client {} closed connection'.format(self.peername))
        self.transport.close()

    def connection_lost(self, exc):
        """ A transport error or EOF is seen which
        means the client is disconnected.

        Inform the pubsub hub that the subscriber has
        dissapeared
        """
        if exc:
            print('{} {}'.format(exc, self.peername))


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
        self.message_queue = queue.Queue
        self.subscribers = {}

    def notify_subscribers(self, channel, message):
        self.message_queue.put({'channel': channel,
                                'message': message})

    def register_subscriber(self, subscriber, channel):
        self.subscribers.setdefault(channel, []).append(subscriber)

    def unregister_subscriber(self, subscriber, channel):
        self.subscribers[channel].remove(subscriber)


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
