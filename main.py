from find_word_count import FindWordCount
from find_word_frequency import FindWordFrequency
import sys

if __name__ == '__main__':

    num_argv = len(sys.argv)

    if num_argv == 4 and sys.argv[1] == 'COUNT':
        count_mapreduce = FindWordCount(int(sys.argv[2]))
        count_mapreduce.start(sys.argv[3])
    elif num_argv == 5 and sys.argv[1] == 'FREQ':
        count_mapreduce = FindWordFrequency(int(sys.argv[2]))
        count_mapreduce.start(sys.argv[3], sys.argv[4])
    else:
        print('You have entered wrong inputs, check your command and parameters')
        print(num_argv, str(sys.argv))
