from map_reduce import MapReduce


class FindWordFrequency(MapReduce):
    def Map(self, map_input):

        filename = map_input["filename"]
        start_index = map_input["startno"]
        end_index = map_input["endno"]
        keyword=map_input["keyword"]

        # read file
        f = open(filename, "r", encoding='utf-8')
        text = f.read()
        line = text[start_index:end_index]
        words=line.split(' ')
        count = 0
        for word in words:
            if word == keyword:
                count+=1

        return count


    def Reduce(self,reduce_input):
        totalCount = 0
        for i in reduce_input:
            totalCount += i

        return totalCount
