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


# A simple example demonstrating use of JSONSerializer.

import argparse
from uuid import uuid4
from six.moves import input
from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
#from confluent_kafka.schema_registry import *
import mysql.connector
from datetime import datetime
import time
import pandas as pd
from typing import List
from sqlalchemy import create_engine


sql = "SELECT * FROM kafka_injection"
columns=['emp_id','emp_name','salary','created_at','updated_at']

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="password",
  database="demo"
)


API_KEY = 'IT3FY6BAPMJD4VKA'
API_SECRET_KEY = '+sy9QkPKj04ngPiM284idc89T6P6u8XXnRhH/Q35HEJLBlo7ZJpmRJ9PPx+bme5K'
BOOTSTRAP_SERVER = 'pkc-xrnwx.asia-south2.gcp.confluent.cloud:9092'
SECURITY_PROTOCOL = 'SASL_SSL'
SSL_MACHENISM = 'PLAIN'
ENDPOINT_SCHEMA_URL  = 'https://psrc-6y63j.ap-southeast-2.aws.confluent.cloud'
SCHEMA_REGISTRY_API_KEY = 'YHV53RZP7OYCHDDM'
SCHEMA_REGISTRY_API_SECRET = 'h6uMDHyR/lSmRpfD5U3Pl2OAlF0Nfli7APhQ7UHaxoju/DaFwCIMouTBjj2yAqsL'


def sasl_conf():

    sasl_conf = {'sasl.mechanism': SSL_MACHENISM,
                 # Set to SASL_SSL to enable TLS support.
                #  'security.protocol': 'SASL_PLAINTEXT'}
                'bootstrap.servers':BOOTSTRAP_SERVER,
                'security.protocol': SECURITY_PROTOCOL,
                'sasl.username': API_KEY,
                'sasl.password': API_SECRET_KEY
                }
    return sasl_conf



def schema_config():
    return {'url':ENDPOINT_SCHEMA_URL,
    
    'basic.auth.user.info':f"{SCHEMA_REGISTRY_API_KEY}:{SCHEMA_REGISTRY_API_SECRET}"

    }


class Car:   
    def __init__(self,record:dict):
        for k,v in record.items():
            setattr(self,k,v)
        
        self.record=record
   
    @staticmethod
    def dict_to_car(data:dict,ctx):
        return Car(record=data)

    def __str__(self):
        return f"{self.record}"


def get_car_instance(sql):

    df = pd.read_sql(sql, con=mydb)

    cars:List[Car]=[]
    for data in df.values:
        data_list = list(data)
        data_list[3] = data_list[3].strftime("%Y-%m-%d %H:%M:%S") # convert created_at to string
        data_list[4] = data_list[4].strftime("%Y-%m-%d %H:%M:%S") # convert updated_at to string
        car=Car(dict(zip(columns,data_list)))
        cars.append(car)
        yield car 


def car_to_dict(car:Car, ctx):
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
    return car.record


def delivery_report(err, msg):
    """
    Reports the success or failure of a message delivery.
    Args:
        err (KafkaError): The error that occurred on None on success.
        msg (Message): The message that was produced or failed.
    """

    if err is not None:
        print("Delivery failed for User record {}: {}".format(msg.key(), err))
        return
    print('User record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))


def main(topic):
    schema_registry_conf = schema_config()
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    # subjects = schema_registry_client.get_subjects()
    # print(subjects)
    subject = topic+'-value'

    schema = schema_registry_client.get_latest_version(subject)
    schema_str=schema.schema.schema_str

    string_serializer = StringSerializer('utf_8')
    json_serializer = JSONSerializer(schema_str, schema_registry_client, car_to_dict)

    producer = Producer(sasl_conf())

    print("Producing user records to topic {}. ^C to exit.".format(topic))
    #while True:
        # Serve on_delivery callbacks from previous calls to produce()
    producer.poll(0.0)
    try:
        for car in get_car_instance(sql):
            print(car)
            producer.produce(topic=topic,
                            key=string_serializer(str(uuid4())),
                            value=json_serializer(car, SerializationContext(topic, MessageField.VALUE)),
                            on_delivery=delivery_report)
            
    except KeyboardInterrupt:
        pass
    except ValueError:
        print("Invalid input, discarding record...")
        pass

    print("\nFlushing records...")
    producer.flush()

main("kafka_topic")