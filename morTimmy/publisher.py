#!/usr/bin/env python3

import random
from libs.pubsub import Publisher
from time import sleep

if __name__ == '__main__':
    publisher = Publisher('127.0.0.1', 10666)

    while True:
        recv_message = publisher.protocol.recv_message()
        if recv_message:
            print(recv_message)
        publisher.notify("sensor", str(random.randint(1, 10)))
        sleep(1)

    publisher.protocol.close()
