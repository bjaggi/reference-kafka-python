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
import os
from uuid import uuid4

from six.moves import input

from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from config import confluent_cluster_config, confluent_sr_config, client_config
from aiokafka import AIOKafkaProducer
import ssl


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
    """
    Reports the failure or success of a message delivery.

    Args:
        err (KafkaError): The error that occurred on None on success.

        msg (Message): The message that was produced or failed.

    Note:
        In the delivery report callback the Message.key() and Message.value()
        will be the binary format as encoded by any configured Serializers and
        not the same object that was passed to produce().
        If you wish to pass the original object(s) for key and value to delivery
        report callback we recommend a bound callback or lambda where you pass
        the objects along.
    """

    if err is not None:
        print("Delivery failed for User record {}: {}".format(msg.key(), err))
        return
    print('User record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))


def producer_stats_cb():
    print("stats_cb called")


async def send_one():
    producer = AIOKafkaProducer(bootstrap_servers=confluent_cluster_config['bootstrap.servers'],
                                security_protocol=confluent_cluster_config['security.protocol'],
                                sasl_mechanism=confluent_cluster_config['sasl.mechanisms'],
                                ssl_context=ssl.SSLContext(ssl.PROTOCOL_TLSv1_2),
                                sasl_plain_username=confluent_cluster_config['sasl.username'],
                                sasl_plain_password=confluent_cluster_config['sasl.password'])

    # Get loop running in current thread
    loop = asyncio.get_running_loop()

    # Start the producer
    await producer.start()
    try:
        while True:
            await producer.send_and_wait("test-asyncio", b"your_message")
            print("sending messa")
    finally:
        # Wait for all pending messages to be delivered or expire.
        await producer.stop()




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

    asyncio.run(send_one())

