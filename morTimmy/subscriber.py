#!/usr/bin/env python3

from libs.pubsub import PubSubClient

if __name__ == '__main__':
    subscriber = PubSubClient('127.0.0.1', 10666)

    subscriber.subscribe("sensor")

    while True:
        recv_message = subscriber.protocol.recv_message()
        if recv_message:
            print(recv_message)
            if recv_message['type'] == 'subscribe':
                print("Succesfully subscribed to channel {}"
                      .format(recv_message['channel']))
                break

    subscriber.unsubscribe("sensor")
    subscriber.protocol.close()
