from pyspark import SparkContext
import numpy as np
from pyspark.mllib.clustering import KMeans, KMeansModel
from clustering.clusterer_interface import Clusterer


class BasicClusterer(Clusterer):

    # Performs k-means clustering and uses elbow method for determining optimal number of clusters in [min, max] range
    @staticmethod
    def perform_training(sc: SparkContext, params_dict: dict):
        normal_ekg_data_path = None if 'normal_ekg_data_path' not in params_dict else params_dict['normal_ekg_data_path']
        min_num_of_clusters = 5 if 'min_num_of_clusters' not in params_dict else int(params_dict['min_num_of_clusters'])
        max_num_of_clusters = 20 if 'max_num_of_clusters' not in params_dict else int(params_dict['max_num_of_clusters'])
        boundary_ratio = 0.8 if 'boundary_ratio' not in params_dict else int(params_dict['boundary_ratio'])

        ekg_rdd_data = sc.textFile(normal_ekg_data_path).map(
            lambda line: np.array([float(val) for val in line.split(',')]))

        k_range = range(min_num_of_clusters, max_num_of_clusters, 2)
        prev_cost = float(np.inf)
        final_km = KMeansModel(ekg_rdd_data.takeSample(False, 1))
        cost_ratios = []
        found_best = False
        for k in k_range:
            km = KMeans.train(ekg_rdd_data, k)
            # cost equals to sum of squared distances of samples to the nearest cluster centre
            cost = km.computeCost(ekg_rdd_data)
            ratio = cost / prev_cost
            prev_cost = cost
            cost_ratios.append(ratio)
            if (ratio > boundary_ratio) & (not found_best):
                final_km = km
                found_best = True

        return final_km
