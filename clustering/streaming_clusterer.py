import json
from pyspark import SparkContext
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.clustering import StreamingKMeans
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from clustering.clusterer_interface import Clusterer


class StreamingClusterer(Clusterer):

    @staticmethod
    def perform_training(sc: SparkContext, params_dict: dict):
        batch_duration = 1 if 'batch_duration' not in params_dict else params_dict['batch_duration']
        training_duration = 20 if 'training_duration' not in params_dict else params_dict['training_duration']
        ssc = StreamingContext(sc, batch_duration)
        topics = ['test-topic']
        kafka_params = {'metadata.broker.list': 'localhost:9092'}
        kvs = KafkaUtils.createDirectStream(ssc, topics, kafkaParams=kafka_params,
                                            valueDecoder=lambda val: json.loads(val.decode('utf-8')))

        windowed_signal = kvs.map(lambda msg: Vectors.dense([float(value) for value in json.loads(msg[1])]))

        model = StreamingKMeans(k=20, decayFactor=1.0).setRandomCenters(188, 0.0, 90)
        model.trainOn(windowed_signal)

        ssc.start()
        ssc.awaitTerminationOrTimeout(training_duration)
        ssc.stop(stopSparkContext=False, stopGraceFully=True)

        return model.latestModel()
