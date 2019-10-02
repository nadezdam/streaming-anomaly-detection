import os
import findspark
from pyparsing import col
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import warnings
from clustering.clusterer_factory import ClustererFactory
from initialization_lib import initialize_logger
from anomaly_detector import AnomalyDetector
from plotting.plotter import Plotter


def parse_data_from_kafka_msg(sdf, schema):
    from pyspark.sql.functions import split
    assert sdf.isStreaming, "DataFrame doesn't receive streaming data"
    # col = split(sdf['value'], ',')  # split attributes to nested array in one Column
    col = sdf['value']
    # now expand col to multiple top-level columns
    for idx, field in enumerate(schema):
        sdf = sdf.withColumn(field.name, col.getItem(idx).cast(field.dataType))
    return sdf.select([field.name for field in schema])


class Consumer:
    @staticmethod
    def run_ekg_consumer(config_dict: dict):
        findspark.init('C:/spark-2.3.3-bin-hadoop2.7')
        sc = SparkContext(appName='SparkEKGConsumer')
        logging_file = None if 'logging_file' not in config_dict else config_dict['logging_file']
        logger = initialize_logger(logging_file)

        clustering_mode = None if 'clustering_mode' not in config_dict else config_dict['clustering_mode']
        normal_ekg_data_file = None if 'normal_ekg_data_file' not in config_dict else config_dict[
            'normal_ekg_data_file']
        normal_ekg_file_path = os.getcwd() + normal_ekg_data_file
        min_num_of_clusters = 5 if 'min_num_of_clusters' not in config_dict else config_dict['min_num_of_clusters']
        max_num_of_clusters = 20 if 'max_num_of_clusters' not in config_dict else config_dict['max_num_of_clusters']
        batch_duration = 1 if 'batch_duration' not in config_dict else int(config_dict['batch_duration'])
        training_duration = 20 if 'training_duration' not in config_dict else int(config_dict['training_duration'])

        clustering_params_dict = dict()
        # basic clusterer params
        clustering_params_dict['normal_ekg_data_path'] = normal_ekg_file_path
        clustering_params_dict['min_num_of_clusters'] = min_num_of_clusters
        clustering_params_dict['max_num_of_clusters'] = max_num_of_clusters
        # streaming clusterer params
        clustering_params_dict['batch_duration'] = batch_duration
        clustering_params_dict['training_duration'] = training_duration

        clusterer = ClustererFactory.get_clusterer(clustering_mode)
        model = clusterer.perform_training(sc, clustering_params_dict)

        clusters = model.clusterCenters
        Plotter.plot_cluster_centers(clusters)
        logger.info('Number of clusters in a model: ' + str(len(clusters)))
        for index, center in enumerate(clusters):
            logger.info('Cluster center ' + str(index) + ' : \n' + str(center) + '\n')

        anomaly_detector = AnomalyDetector(model)

        # Spark DStream
        AnomalyDetector.perform_anomality_check_dstream(anomaly_detector, sc)

        # Spark structured streaming
        # AnomalyDetector.perform_anomality_check_structured_stream(anomaly_detector)

