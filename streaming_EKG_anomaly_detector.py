import threading
from initialization_lib import load_configuration
from streaming_EKG_consumer import Consumer
from streaming_EKG_producer import Producer

if __name__ == "__main__":
    producer_config_dict = load_configuration('ProducerConfig')
    consumer_config_dict = load_configuration('ConsumerConfig')

    # TODO change the order of running producer and consumer
    producer_thread = threading.Thread(target=Producer.run_ekg_stream_producer, args=[producer_config_dict])
    producer_thread.start()

    Consumer.run_ekg_consumer(consumer_config_dict)
