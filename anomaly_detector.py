import json
import scipy
from pyparsing import col
from pyspark import SparkContext
from pyspark.mllib.clustering import StreamingKMeansModel
from pyspark.mllib.linalg import Vectors
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from scipy.spatial import distance

from plotting.plotter import Plotter


class AnomalyDetector:

    def __init__(self, model: StreamingKMeansModel) -> None:
        self.model = model

    @staticmethod
    def check_anomality(self, signal_window):
        closest_center_index = self.model.predict(signal_window)
        closest_center = self.model.centers[closest_center_index]
        diff = distance.euclidean(closest_center, signal_window)
        # correlation_coeff = distance.correlation(closest_center, window)
        # diff = closest_center - window
        # mean = scipy.mean(diff)
        outcome = 'Euclidean distance between closest cluster center and signal: ' + str(diff) + '\n'
        # outcome = 'Correlation distance: ' + str(correlation_coeff) + '\n'
        if diff > 2:
            outcome += 'ANOMALY DETECTED!\n'
            file_path = Plotter.plot_distances(closest_center, signal_window, diff, is_anomaly=True)
        else:
            outcome += 'Heartbeat is in normal range \n'
            file_path = Plotter.plot_distances(closest_center, signal_window, diff, is_anomaly=False)
        outcome += 'Signal saved to: ' + file_path
        return outcome

    @staticmethod
    def perform_anomality_check_dstream(self, sc: SparkContext, batch_duration: int = 1):
        ssc = StreamingContext(sc, batch_duration)
        topics = ['ekg-stream']
        kafka_params = {'metadata.broker.list': 'localhost:9092'}

        kvs = KafkaUtils.createDirectStream(ssc, topics, kafkaParams=kafka_params,
                                            valueDecoder=lambda val: json.loads(val.decode('utf-8')))

        windowed_signals = kvs.map(lambda msg: Vectors.dense([float(value) for value in msg[1]['signal_values']]))

        predictions = windowed_signals.map(lambda window: AnomalyDetector.check_anomality(self, window))
        predictions.pprint()
        ssc.start()
        ssc.awaitTermination()

    @staticmethod
    def perform_anomality_check_structured_stream(self):

        process_row_udf = udf(lambda heart_beat: AnomalyDetector.check_anomality(self, heart_beat))

        spark_session = SparkSession.builder \
            .appName('EKG structured streaming') \
            .getOrCreate()
        ekg_signal_schema = StructType(
            [StructField('timestamp', StringType()),
             StructField('signal_values', ArrayType(FloatType()))
             ])

        ekg_stream_df = spark_session \
            .readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe', 'ekg-stream') \
            .option('startingOffsets', 'earliest') \
            .load() \
            .select(from_json(col('value').cast('string'), ekg_signal_schema).alias('value'))

        parsed_ekg_stream_df = ekg_stream_df.select(['value.timestamp', 'value.signal_values'])
        # .withColumn('signal_values', process_row_udf('signal_values'))
        parsed_ekg_stream_df.printSchema()
        query = parsed_ekg_stream_df \
            .writeStream \
            .format('console') \
            .option('truncate', False) \
            .start()

        query.awaitTermination()
