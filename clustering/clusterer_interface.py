from abc import abstractstaticmethod
from pyspark import SparkContext


class Clusterer:
    @abstractstaticmethod
    def perform_training(sc: SparkContext, params_dict: dict):
        pass
