from abc import ABC, abstractmethod
from multiprocessing import Process, Value, Array
import zmq, os


class MapReduce(ABC):
    def __init__(self, NumWorker):
        self.numWorker = NumWorker
        self.filename  = ''

    # Method will be overidden by subclasses
    @abstractmethod
    def Map(self, map_input):
        pass

    # Method will be overidden by subclasses
    @abstractmethod
    def Reduce(self, reduce_input):
        pass

    # Producer creates workers and maps whole job into those Consumers
    def __Producer(self, producer_input):
        print('Producer process id:', os.getpid())
        context = zmq.Context()
        socket = context.socket(zmq.PUSH)
        socket.connect("tcp://127.0.0.1:5558")
        char_count = len(producer_input)
        word_range = char_count//self.numWorker
        startno, endno = 0, word_range

        for i in range(self.numWorker-1):
            while producer_input[endno-1] != ' ':
                endno += 1

            work_message = {'startno': startno, 'endno': endno}
            socket.send_json(work_message)

            startno = endno
            if (endno + word_range) < char_count:
                endno += word_range
            else:
                endno = char_count

    # Create Workers which consume given by Produces
    # Sends to the result Result Collector
    def __Consumer(self):
        print('Consumer process id:', os.getpid())
        context = zmq.Context()
        consumer_receiver = context.socket(zmq.PULL)
        consumer_receiver.connect("tcp://127.0.0.1:5558")

        consumer_sender = context.socket(zmq.PUSH)
        consumer_sender.connect("tcp://127.0.0.1:5559")

        index = consumer_receiver.recv_json()

        map_input = {'filename': self.filename, 'startno': index['startno'], 'endno': index['endno']}
        result = self.Map(map_input)
        result = {'partial_result': result}
        consumer_sender.send_json(result)

    # Collects results from Consumers reduces it
    def __ResultCollector(self):
        context = zmq.Context()
        collector_receiver = context.socket(zmq.PULL)
        collector_receiver.connect("tcp://127.0.0.1:5559")
        list_result = []
        for i in range(self.numWorker):
            res_json = collector_receiver.recv_json()
            list_result.append(res_json['partial_result'])

        print('Result: ' + self.Reduce(list_result))


    # Create 1 Producer, 1 Collector, NumWorker Consumers
    # Orchestrate all process
    def start(self, filename, keyword=None):
        self.filename = filename
        content = open(filename, 'r').read()
        producer = Process(target=self.__Producer, args=(content,))
        producer.start()

        for i in range(self.numWorker):
            Process(target=self.__Consumer()).start()

        collector = Process(target=self.__ResultCollector())
        collector.start()

        producer.join()
        collector.join()

