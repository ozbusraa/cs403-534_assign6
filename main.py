from find_word_count import FindWordCount
from find_word_frequency import FindWordFrequency
import sys

if __name__ == '__main__':

    num_argv = len(sys.argv)
    list_argv = str(sys.argv)

    if num_argv == 4 and list_argv[1] == 'COUNT':
        count_mapreduce = FindWordCount(int(list_argv[2]))
        count_mapreduce.start(list_argv[3])
    elif num_argv == 5 and list_argv[1] == 'FREQ':
        count_mapreduce = FindWordFrequency(int(list_argv[2]))
        count_mapreduce.start(list_argv[3], list_argv[4])
    else:
        print('You have entered wrong inputs, check your command and parameters')
