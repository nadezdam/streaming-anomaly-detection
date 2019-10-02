from datetime import datetime

import matplotlib.pyplot as plt
import numpy as np


class Plotter:
    @staticmethod
    def plot_elbow(cost_ratios, k_range):
        x_vals = np.asarray(k_range)
        y_vals = (np.asarray(cost_ratios))
        plt.plot(x_vals, y_vals)
        plt.title('Elbow results')
        plt.xlabel('Number of clusters')
        plt.ylabel('Cost ratio')
        plt.savefig('./plots/elbow_method_result')
        plt.clf()

    @staticmethod
    def plot_distances(cluster_center, signal, diff_coeff, is_anomaly: bool):
        num_of_points = len(signal)
        t = np.arange(0, num_of_points)
        plt.plot(t, signal)
        plt.plot(t, cluster_center)
        plt.title('Euclidean distance: ' + str(diff_coeff))
        plt.xlabel('1 second values')
        plt.ylabel('signal value')
        plt.legend(['signal', 'closest_center'], loc='upper left')
        file_path = './plots/'
        if is_anomaly:
            file_path += 'anomalies/'
        else:
            file_path += 'normal/'
        full_file_path=file_path + datetime.now().strftime('%H-%M-%S')
        plt.savefig(full_file_path)
        plt.clf()
        return full_file_path

    @staticmethod
    def plot_signal_window(signal, index):
        num_of_points = len(signal)
        t = np.arange(0, num_of_points)
        plt.plot(t, signal)
        plt.xlabel('1 second values')
        plt.ylabel('signal value')
        plt.savefig('./plots/signals/ ' + str(index))
        plt.clf()

    @staticmethod
    def plot_cluster_centers(cluster_centers):
        for index, center in enumerate(cluster_centers):
            num_of_points = len(center)
            t = np.arange(0, num_of_points)
            plt.plot(t, center)
            plt.xlabel('1 second values')
            plt.ylabel('signal value')
            plt.savefig('./plots/cluster_centers/center ' + str(index))
            plt.clf()
