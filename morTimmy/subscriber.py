#!/usr/bin/env python3

from pubsub import Subscriber
from time import sleep

if __name__ == '__main__':
    subscriber = Subscriber('127.0.0.1', 10666)

    subscriber.subscribe("sensor")

    while True:
        recv_message = subscriber.protocol.recv_message()
        if recv_message:
            if recv_message['type'] == 'subscribe':
                print("Succesfully subscribed to channel {}"
                      .format(recv_message['channel']))

    subscriber.protocol.close()
