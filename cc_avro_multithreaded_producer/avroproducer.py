#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2020 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


# A simple example demonstrating use of AvroSerializer.

import argparse
import asyncio
import threading
import schedule
import os
from uuid import uuid4

from six.moves import input

from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from config import confluent_cluster_config, confluent_sr_config, client_config
import ssl
import time


class User(object):
    """
    User record

    Args:
        name (str): User's name

        favorite_number (int): User's favorite number

        favorite_color (str): User's favorite color

        address(str): User's address; confidential
    """

    def __init__(self, name, address, favorite_number, favorite_color):
        self.name = name
        self.favorite_number = favorite_number
        self.favorite_color = favorite_color
        # address should not be serialized, see user_to_dict()
        self._address = address


def user_to_dict(user, ctx):
    """
    Returns a dict representation of a User instance for serialization.

    Args:
        user (User): User instance.

        ctx (SerializationContext): Metadata pertaining to the serialization
            operation.

    Returns:
        dict: Dict populated with user attributes to be serialized.
    """

    # User._address must not be serialized; omit from dict
    return dict(name=user.name,
                favorite_number=user.favorite_number,
                favorite_color=user.favorite_color)


def delivery_report(err, msg):
    if err:
        print("Delivery failed for User record {}: {}".format(msg.key(), err))
    else:
        print('User record {} successfully produced to {} [{}] at offset {}'.format(
            msg.key(), msg.topic(), msg.partition(), msg.offset()))


def producer_stats_cb():
    print("stats_cb called")


def send_messages(producer_local, name):
    user_name = "jaggi"
    user_address = "home"
    user_favorite_number = 1
    user_favorite_color = "blue"
    user = User(name=user_name,
                address=user_address,
                favorite_color=user_favorite_color,
                favorite_number=user_favorite_number)
    # Serve on_delivery callbacks from previous calls to produce()
    producer_local.poll(1.0)
    try:
        message_key = str(uuid4())
        producer_local.produce("test-multithreading", key=message_key,
                               value=avro_serializer(user, SerializationContext(topic, MessageField.VALUE)),
                               on_delivery=delivery_report)

        print("sending message, using Thread " + name + " " + message_key)
    except KeyboardInterrupt:
        print("\nJob was killed, Flushing records...")
        producer_local.flush(100)
    except Exception as e:
        print("\nUnexpected exception , Flushing records..." + e )
        producer_local.flush(100)






def start_multithreaded_producer(name):
    producer_local = Producer(confluent_cluster_config)

    while True:
        send_messages(producer_local, name)
        #time.sleep(1)


if __name__ == '__main__':
    topic = client_config.get('topic_name')

    schema = "user.avsc"

    path = os.path.realpath(os.path.dirname(__file__))
    with open(f"{path}/../avro_schemas/{schema}") as f:
        schema_str = f.read()

    schema_registry_client = SchemaRegistryClient(confluent_sr_config)

    avro_serializer = AvroSerializer(schema_registry_client,
                                     schema_str,
                                     user_to_dict)

    string_serializer = StringSerializer('utf_8')

    thread1 = threading.Thread(target=start_multithreaded_producer, args=("Thread-oneeee",))
    process2 = threading.Thread(target=start_multithreaded_producer, args=("Thread-twooo",))
    process3 = threading.Thread(target=start_multithreaded_producer, args=("Thread-three",))

    thread1.start()
    process2.start()
    process3.start()

    thread1.join()
    process2.join()
    process3.start()
