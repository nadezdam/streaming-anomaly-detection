from clustering.basic_clusterer import BasicClusterer
from clustering.streaming_clusterer import StreamingClusterer


class ClustererFactory:
    @staticmethod
    def get_clusterer(clusterer_type: str):
        clusterers = {
            'basic': BasicClusterer(),
            'streaming': StreamingClusterer()
        }

        return clusterers[clusterer_type]
