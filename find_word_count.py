from map_reduce import MapReduce


# implement
class FindWordCount(MapReduce):
    def Map(self, map_input):

        filename=map_input["filename"]
        start_index=map_input["startno"]
        end_index=map_input["endno"]

        #read file
        f= open(filename,"r",encoding='utf-8')
        text=f.read()

        line=text[start_index:end_index]
        return len(line.split(' '))

    def Reduce(self,reduce_input):
        result=0
        for i in reduce_input:
            result+=i
        return result
