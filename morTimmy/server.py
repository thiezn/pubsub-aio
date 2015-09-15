#!/usr/bin/env python3

import asyncio
import json
import logging
from connector import Connector


class PubSubConnector(Connector):
    """ Provides an interface between the clients and pubsubhub server """

    def __init__(self, event_loop):
        """ Initialises the objects with an empty connections cache. This
        will hold the protocol objects associated with peers. The passed in
        event_loop is used to craete connections to the remote peers in the
        network
        """
        self._connections = {}
        self.event_loop = event_loop

    def _send_message_with_protocol(self, message, protocol):
        """ Sends the message to the remote peer using the
        supplied protocol object """
        protocol.send_string(message)

    def send(self, peer, message, sender):
        """ Sends a message to the referenced peer

        Args:
            peer:       PeerNode object
            message:    message to send
            sender:     The sender
        """
        delivered = asyncio.Future()
        if peer.network_id in self._connections:
            # Use the cached protocol object
            protocol = self._connections[peer.network_id]
            try:
                self._send_message_with_protocol(message, protocol)
                delivered.set_result(True)
                return delivered
            except:
                # Continue and retry with a fresh connection. E.G. perhaps the
                # transport dropped but the remote peer is still online.
                # clean up the old protocol object
                del self._connections[peer.network_id]
        # Create a new connection and cache it for future use
        protocol = lambda: PubHubServerProtocol(self, sender)
        coro = self.event_loop.create_connection(protocol, peer.address,
                                                 peer.port)
        connection = asyncio.Task(coro)

        def on_connect(task, peer=peer, message=message, connector=self,
                       delivered=delivered):
            """ Once a connection is established handle the sending of the
            message and caching of protocol for later use """
            try:
                if task.result():
                    protocol = task.result()[1]
                    connector._send_message_with_protocol(message, protocol)
                    connector._connections[peer.network_id] = protocol
                    delivered.set_result(True)
            except Exception as ex:
                # There was a problem so pass up the callback chain
                # for upstream to handle what to do. (e.g. punish
                # the problem remote peer
                delivered.set_exception(ex)

        connection.add_done_callback(on_connect)
        return delivered

    def recv(self, data, sender, handler, protocol):
        """ Called when a message is received from a remote node on the
        network

        Args:
            data:       The received raw message data
            sender:     The address, port tuple of the sender
            handler:    The local node that received the message
            protocol:   The protocol of the local node
        """
        try:
            message = data
            network_id = sender.network_id
            reply = handler.message_received(message, protocol,
                                             sender.address, sender.port)

            if reply:
                self._send_message_with_protocol(reply, protocol)
            # Cache the connection
            if network_id not in self._connections:
                # if the remote node is a new peer cache the protocol
                self._connections[network_id] = protocol
            elif self._connections[network_id] != protocol:
                # If the remote node has a cached protocol that appears to
                # have expired, replace the stored protocol
                self._connections[network_id] = protocol
        except Exception as ex:
            # Lets try and catch any errors, need to see what kind of
            # things we can expect
            print(ex)


class PubHubServerProtocol(asyncio.Protocol):
    """ A Server Protocol listening for subscriber messages """

    def __init__(self, connector, node):
        """ Connector mediates between the node and the Protocol.
        Node is the local node on the network that handles incoming
        messages """
        self._connector = connector
        self._node = node

    def message_received(self, data):
        """ Process the raw data with the connector and local node 

        TODO: What are we going to do with the Data?
        """
        print(data)

    def connection_made(self, transport):
        """ Called when connection is initiated to this node """

        print("connection from {}".format(self.peername))
        logging.info('connection from {}'.format(self.peername))
        self.transport = transport

    def serialize_msg(self, message):
        """ This function takes a dictionary
        and serializes it to a message ready to send
        """
        msg = json.dumps(message, separators=(',', ':')) + "\r\n"
        return msg.encode('utf-8')

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
        self.message_received(data)

    def send_string(self, data):
        """ encodes and sends a string of data to the node at the
        other end of self.transport. """
        message = json.dumps(data, separators=(',', ':')) + "\r\n"
        return message.encode('utf-8')


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
