from __future__ import print_function
import logging
import json
import time
import datetime

from kafka import KafkaProducer

from plotting.plotter import Plotter


class Producer:
    @staticmethod
    def run_ekg_stream_producer(config_dict):
        topic = None if 'topic' not in config_dict else config_dict['topic']
        input_ekg_file = None if 'input_ekg_file' not in config_dict else config_dict['input_ekg_file']
        partition = None if 'partition' not in config_dict else int(config_dict['partition'])
        key = None if 'key' not in config_dict else config_dict['key']
        bootstrap_servers = 'localhost:9092' if 'bootstrap_servers' not in config_dict else config_dict[
            'bootstrap_servers']
        logging_file = None if 'logging_file' not in config_dict else config_dict['logging_file']
        logging.basicConfig(filename=logging_file, filemode='w',
                            format='%(asctime)s - %(levelname)s : %(message)s',
                            datefmt='%d-%b-%y %H:%M:%S',
                            level=logging.INFO)
        producer = Producer.configure_producer(bootstrap_servers)
        with open(input_ekg_file) as f:
            ekg_lines = [line.rstrip('\r\n') for line in f]
            for index, ekg_signal in enumerate(ekg_lines):
                json_ekg_signal_values = dict()
                json_ekg_signal_values['timestamp'] = datetime.datetime.now().strftime('%m/%d/%Y %H:%M:%S')
                ekg_signal_values = [float(value) for value in ekg_signal.split(',')]
                # Plotter.plot_signal_window(np.asarray(ekg_signal_values, dtype=np.float32), index)
                json_ekg_signal_values['signal_values'] = ekg_signal_values
                producer.send(topic=topic, key=key, value=json_ekg_signal_values, partition=partition)
                producer.flush()
                logging.info('Sent ekg signal message: ' + json.dumps(json_ekg_signal_values))
                time.sleep(1)

        producer.close()

    @staticmethod
    def configure_producer(bootstrap_servers) -> KafkaProducer:
        return KafkaProducer(bootstrap_servers=bootstrap_servers,
                             key_serializer=str.encode,
                             value_serializer=lambda m: json.dumps(m).encode('utf-8'))
