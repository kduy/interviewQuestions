import avro.schema
import avro.io
from kafka.client import KafkaClient
from kafka.producer import SimpleProducer, KeyedProducer
from random import randint
import uuid, io, json, argparse


parser = argparse.ArgumentParser(description='Push Avro messages into Kafka')
parser.add_argument("-m", "-maxMessages", type=int,
                  help="max number of avro messages to write to kafka", default=1000000)
parser.add_argument("-k", "-kafkaConnect",
                  help="Kafka connect string", default="localhost:9092")
parser.add_argument("-t", "-topic",
                  help="Kafka topic", default="neverwinter")
parser.add_argument("-q", "-quiet", action='store_true',
                  help="Disable output to stdout")


args         = vars(parser.parse_args())
maxRecords   = args["m"]
kafkaConnect = args["k"]
topic        = args["t"]
quiet        = args["q"]


schema = avro.schema.parse(json.dumps({
    'name': 'kafkatest',
    'namespace': 'test',
    'type': 'record',
    'fields': [
        {'name': 'id', 'type': 'int'},
        {'name': 'random', 'type': 'int'},
        {'name': 'data', 'type': 'string'},
    ],
  }))


kafka = KafkaClient(kafkaConnect)
kafka.ensure_topic_exists(topic)
producer = SimpleProducer(kafka)

for x in xrange(maxRecords):
  writer = avro.io.DatumWriter(schema)
  bytes_writer = io.BytesIO()
  encoder = avro.io.BinaryEncoder(bytes_writer)
  
  writer.write( {'id': x, 'random': randint(1, 3) ,'data': str(uuid.uuid4().get_hex().upper()[0:20])}, encoder)
  raw_bytes = bytes_writer.getvalue()
  producer.send_messages(topic, raw_bytes)
  if not quiet:
    print "Sent message ID: "+str(x)
