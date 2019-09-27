import os
import findspark
from pyspark import SparkContext
from clustering.clusterer_factory import ClustererFactory
from initialization_lib import initialize_logger
from anomaly_detector import AnomalyDetector


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

        logger.info('Number of clusters in a model: ' + str(len(clusters)))
        for index, center in enumerate(clusters):
            logger.info('Cluster center ' + str(index) + ' : \n' + str(center) + '\n')

        AnomalyDetector.perform_anomality_check(sc, model)
