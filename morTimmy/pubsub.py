#!/usr/bin/env python

# This library provides a design pattern based on the
# publish-subscribe model.
#
# Further reading:
# http://www.slideshare.net/ishraqabd/publish-subscribe-model-overview-13368808


class Publisher:
    """ TODO: Create a interface layer class
    to be able to exchange traffic with the message
    handler. The idea is that we have options for
    serial and socket interfaces """

    def __init__(self, pub_client_protocol):
        self.pub_client_protocol = pub_client_protocol

    def publish(self, message):
        self.pub_client_protocol.notify_subscribers("test")
        # self.provider.notify_subscribers(message)


class Subscriber:
    def __init__(self, name, message_handler):
        self.name = name
        self.message_handler = message_handler

    def subscribe(self, message):
        self.message_handler.register_subscriber(message, self)

    def run(self, message):
        print("{} got {}".format(self.name, message))



if __name__ == '__main__':
    pub_sub_hub = PubSubHub()

