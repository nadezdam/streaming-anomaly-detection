import json
import scipy
from pyspark import SparkContext
from pyspark.mllib.clustering import StreamingKMeansModel
from pyspark.mllib.linalg import Vectors
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils


class AnomalyDetector:
    @staticmethod
    def perform_anomality_check(sc: SparkContext, model: StreamingKMeansModel, batch_duration: int = 1):
        ssc = StreamingContext(sc, batch_duration)
        topics = ['test-topic']
        kafka_params = {'metadata.broker.list': 'localhost:9092'}

        kvs = KafkaUtils.createDirectStream(ssc, topics, kafkaParams=kafka_params,
                                            valueDecoder=lambda val: json.loads(val.decode('utf-8')))

        windowed_signals = kvs.map(lambda msg: Vectors.dense([float(value) for value in json.loads(msg[1])]))

        predictions = windowed_signals.map(lambda window: AnomalyDetector.check_anomality(model, window))
        # TODO send predictions to another Kafka topic for visualization
        predictions.pprint()
        ssc.start()
        ssc.awaitTermination()

    # TODO: improve method for detecting anomalies
    # TODO: change returning result to noisy signal with alarm
    # TODO: change to spark UDF
    @staticmethod
    def check_anomality(model, window):
        closest_center_index = model.predict(window)
        closest_center = model.centers[closest_center_index]
        diff = closest_center - window
        outcome = 'Cluster center: ' + str(closest_center) + '\n'
        outcome += 'Signal window: ' + str(window) + '\n'
        outcome += 'Noisy signal: ' + str(diff) + '\n'
        mean = scipy.mean(diff)
        outcome = 'Mean value of noisy signal: ' + str(mean) + '\n'
        if mean > 1:
            outcome += 'ANOMALY DETECTED!'
        else:
            outcome += 'Heart rate is in normal range'
        return outcome
