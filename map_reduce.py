from abc import ABC, abstractmethod
from multiprocessing import Process, Value, Array
from threading import Thread
import time, random
import zmq, os


class MapReduce(ABC):
    def __init__(self, NumWorker):
        self.keyword = ''
        self.numWorker = NumWorker
        self.filename = ''

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
        socket.bind("tcp://127.0.0.1:5153")
        char_count = len(producer_input)
        word_range = char_count // self.numWorker
        startno, endno = 0, word_range
        time.sleep(1)

        for i in range(self.numWorker):
            while endno < char_count and not (producer_input[endno] == ' ' or producer_input[endno + 1] == '\n'):
                endno += 1

            work_message = {'startno': startno, 'endno': endno}
            socket.send_json(work_message)
            # time.sleep(1)
            print('Producer send ', i, 'th message with', startno, endno)
            startno = endno + 1
            if (endno + word_range) < char_count:
                endno += word_range
            else:
                endno = char_count

    # Create Workers which consume given by Produces
    # Sends to the result Result Collector
    def __Consumer(self,filename, keyword):
        print('Consumer process id:', os.getpid())
        context = zmq.Context()
        consumer_receiver = context.socket(zmq.PULL)
        consumer_receiver.connect("tcp://127.0.0.1:5153")

        consumer_sender = context.socket(zmq.PUSH)
        consumer_sender.connect("tcp://127.0.0.1:5159")
        # for i in range(self.numWorker):
        index = consumer_receiver.recv_json()
        print('Consumer received index')

        map_input = {'filename': filename, 'startno': index['startno'], 'endno': index['endno'], 'keyword': keyword}
        result = self.Map(map_input)
        print('Consumer calculated partial result=', result, ' for index:', index['startno'], index['endno'])
        result = {'partial_result': result}
        consumer_sender.send_json(result)

    # Collects results from Consumers reduces it
    def __ResultCollector(self, numWorker):
        print('Collector process id:', os.getpid())
        context = zmq.Context()
        collector_receiver = context.socket(zmq.PULL)
        collector_receiver.bind("tcp://127.0.0.1:5159")
        list_result = []
        print('Collector is connected')
        for i in range(numWorker):
            res_json = collector_receiver.recv_json()
            print('Result ', i, 'th recieved', res_json['partial_result'])
            list_result.append(res_json['partial_result'])

        print('Result: ', self.Reduce(list_result))

    # Create 1 Producer, 1 Collector, NumWorker Consumers
    # Orchestrate all process
    def start(self, filename, keyword=None):
        self.filename = filename
        self.keyword = keyword
        content = open(filename, 'r').read()

        # ###########TESTS##########
        # words = content.split()
        # print('Word Count:', len(words))
        # freq = 0
        # for w in words:
        #     if w == keyword:
        #         freq += 1
        # print('Freq:', freq)

        producer = Process(target=self.__Producer, args=(content,))
        producer.start()

        workers = []
        for i in range(self.numWorker):
            p = Process(target=self.__Consumer, args=(filename, keyword))
            workers.append(p)
            p.start()
        print('Producer and All Consumers are created')
        collector = Process(target=self.__ResultCollector, args=(self.numWorker,))
        collector.start()

        # producer.join()
        # for process in workers:
        #     process.join()
        # collector.join()



