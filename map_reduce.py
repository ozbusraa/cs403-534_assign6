from abc import ABC, abstractmethod


class MapReduce(ABC):
    def __init__(self, NumWorker):
        self.numWorker = NumWorker

    # Method will be overidden by subclasses
    @abstractmethod
    def Map(self, map_input):
        pass

    # Method will be overidden by subclasses
    @abstractmethod
    def Reduce(self):
        pass

    # Producer creates workers and maps whole job into those Consumers
    def __Producer(self,producer_input):
        pass

    # Create Workers which consume given by Produces
    # Sends to the result Result Collector
    def __Consumer(self):
        pass

    # Collects results from Consumers reduces it
    def __ResultCollector(self):
        pass

    # Create 1 Producer, 1 Collector, NumWorker Consumers
    # Orchestrate all process
    def start(self, filename, keyword=None):
        pass


