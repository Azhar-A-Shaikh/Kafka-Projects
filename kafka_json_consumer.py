import argparse
from confluent_kafka import Consumer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider,SimpleStatement
from datetime import datetime


cloud_config= {
  'secure_connect_bundle': 'C:\Users\Azhar\Desktop\kafka class\secure-connect-cassandra-demo.zip'
}
auth_provider = PlainTextAuthProvider('yoOLZqIIZRtAsWlHDnwPfWXJ', 'xUBElnC4YPTm_+.EhjpJAZ9JhEbWdv2sCJ_3M2Okul.mF1S5_68Cv1rBqGYr_MIhq+r60r1-l0sjMnWBrL8A1W_OiCT5mxh3CRE6Dw,pY15iNTW.jRg7U9ZpzypShqBi')
cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
session = cluster.connect()

try:
    query = "use employee_keyspace"
    session.execute(query)
    print("Inside the employee_keyspace")
except Exception as err:
    print("Exception Occured while using Keyspace : ",err)


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


def write_to_cassandra(emp_id, emp_name, salary):
    cluster = Cluster(['127.0.0.1']) # replace with your Cassandra cluster IP address
    session = cluster.connect()

    # create keyspace and table
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS employee_keyspace
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
        """)
    session.execute("""
        CREATE TABLE IF NOT EXISTS employee_keyspace.kafka_injection (
        emp_id int,
        emp_name text,
        salary int,
        created_at timestamp,
        updated_at timestamp,
        )
        """)

    # insert data into table
    session.execute("""
        INSERT INTO employee_keyspace.kafka_injection (emp_id, emp_name, salary, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s)
        """, (emp_id, emp_name, salary, datetime.now(), datetime.now()))


def main(topic):
    # Cassandra configuration
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect()
    session.execute("CREATE KEYSPACE IF NOT EXISTS kafka WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}")
    session.execute("CREATE TABLE IF NOT EXISTS kafka.kafka_injection(emp_id int, emp_name text, salary int, created_at timestamp, updated_at timestamp)")

    schema_registry_conf = schema_config()
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    subject = topic + '-value'
    schema = schema_registry_client.get_latest_version(subject)
    schema_str = schema.schema.schema_str
    json_deserializer = JSONDeserializer(schema_str, from_dict=Car.dict_to_car)

    consumer_conf = sasl_conf()
    consumer_conf.update({
        'group.id': 'group1',
        'auto.offset.reset': 'latest'
    })
    consumer = Consumer(consumer_conf)
    consumer.subscribe([topic])

    while True:
        try:
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            car = json_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
            if car is not None:
                # Transform record
                emp_id = car.record['id']
                emp_name = car.record['name']
                salary = car.record['salary']
                created_at = datetime.now()
                updated_at = datetime.now()

                # Write to Cassandra
                insert_statement = SimpleStatement("INSERT INTO kafka.kafka_injection(emp_id, emp_name, salary, created_at, updated_at) VALUES (%s, %s, %s, %s, %s)")
                session.execute(insert_statement, (emp_id, emp_name, salary, created_at, updated_at))
                print(f"Record written to Cassandra: {car}")

        except KeyboardInterrupt:
            break

    consumer.close()
    cluster.shutdown()

main("kafka_topic")
