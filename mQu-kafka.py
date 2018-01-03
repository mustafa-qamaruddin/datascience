import threading
import logging
import time
import json

from kafka import KafkaConsumer, KafkaProducer

class Producer(threading.Thread):
    daemon = True

    def acked(self, err, msg):
        if err is not None:
            print("Failed to deliver message: {0}: {1}".format(msg.value(), err.str()))
        else:
            print("Message produced: {0}".format(msg.value()))

    def run(self):
        producer = KafkaProducer(bootstrap_servers='localhost:9092',
                                 request_timeout_ms=1200000,
                                 value_serializer=lambda v: json.dumps(v).encode('utf-16'))

        for i in range(0, 10):
            producer.send(topic='my-topic', value={"id": "%d"%(i)}, key="%d"%(i))
            time.sleep(1)

class Consumer(threading.Thread):
    daemon = True
    id = 0

    def __init__(self, _id):
        super(Consumer,self).__init__()
        threading.Thread.__init__(self)
        self.id = _id

    def run(self):
        consumer = KafkaConsumer(bootstrap_servers='localhost:9092',
                                 auto_offset_reset='earliest',
                                 enable_auto_commit=True,
                                 request_timeout_ms=1200000,
                                 session_timeout_ms=600000,
                                 value_deserializer=lambda m: json.loads(m.decode('utf-16')))
        consumer.subscribe(['my-topic'])

        for msg in consumer:
            print ( "CONSUMER: %d" % self.id )
            print (msg)

def main():
    threads = [
        Producer(),
        Consumer(1),
        Consumer(2),
        Consumer(3),
        Consumer(4)
    ]

    for t in threads:
        t.start()

if __name__ == "__main__":
    logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:' +
               '%(levelname)s:%(process)d:%(message)s',
        level=logging.INFO
    )
    main()
