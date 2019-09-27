from __future__ import print_function
import logging
import json
import time
from kafka import KafkaProducer


class Producer:
    @staticmethod
    def run_ekg_stream_producer(config_dict):
        topic = None if 'topic' not in config_dict else config_dict['topic']
        input_ekg_file = None if 'input_ekg_file' not in config_dict else config_dict['input_ekg_file']
        partition = None if 'partition' not in config_dict else int(config_dict['partition'])
        key = None if 'key' not in config_dict else config_dict['key']
        logging_file = None if 'logging_file' not in config_dict else config_dict['logging_file']
        logging.basicConfig(filename=logging_file, filemode='w',
                            format='%(asctime)s - %(levelname)s : %(message)s',
                            datefmt='%d-%b-%y %H:%M:%S',
                            level=logging.INFO)
        producer = Producer.configure_producer()
        with open(input_ekg_file) as f:
            ekg_lines = [line.rstrip('\r\n') for line in f]
            for ekg_signal in ekg_lines:
                ekg_signal_values = ekg_signal.split(',')
                json_ekg_signal_values = json.dumps(ekg_signal_values)
                producer.send(topic=topic, key=key, value=json_ekg_signal_values, partition=partition)
                producer.flush()
                logging.info('Sent ekg signal message: ' + json_ekg_signal_values)
                time.sleep(1)

        producer.close()

    @staticmethod
    def configure_producer() -> KafkaProducer:
        bootstrap_servers = 'localhost:9092'
        return KafkaProducer(bootstrap_servers=bootstrap_servers,
                             key_serializer=str.encode,
                             value_serializer=lambda m: json.dumps(m).encode('utf-8'))
