#!/usr/bin/env python3


class Connector:
    """ The base class for all connectors. Handles network connectivity and
    the sending and receiving of messages. They should only implement two
    methods: send and receive
    """

    def __init__(self, event_loop):
        """ Instantiates the class with a reference to the asyncio loop
        used to create connections to remote peers.
        """
        self.event_loop = event_loop

    def send(self, contact, message):
        """ Sends the message instance to the referenced contact """
        raise NotImplementedError()

    def recv(self, message, sender, handler, protocol):
        """ Receives a raw message from a sender

        Args:
            message:    The received message
            sender:     The sender of the message
            handler:    A reference to the receiving node
            protocol:   A reference to the receiving node's protocol
        """
        raise NotImplementedError()
