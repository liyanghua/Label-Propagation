import random, collections, array
from multiprocessing import cpu_count

class AdjDict(dict):
    """
     A special Dictionary Class to hold adjecany list
    """
    def __missing__(self, key):
        """
        Missing is changed such that when a key is not found an integer array is initialized
        """
        self.__setitem__(key,array.array('i'))
        return self[key]


def maxVote(nLabels):
    """
    This function is used byt map function, given a list of labels of neighbors
    this function finds the most frequent labels and randomly returns one of them
    """
    cnt = collections.defaultdict(int)
    for i in nLabels:
        cnt[i] += 1
    maxv = max(cnt.itervalues())
    return random.choice([k for k,v in cnt.iteritems() if v == maxv])

def ParseOptions(argv):
    """
    Parse the Command line Options
    """
    THREADS = cpu_count() # Variable THREAD defines number of processes to be used
    
    if len(argv)>1:
        filename = argv[1] 
    else:
        'Please specify file name E.g. LP.py Network.txt'

    if len(argv)>2:
        iterations = int(argv[2]) 
    else:
        print " Number of Iterations not specified, using 5 iterations "
        print " specify iterations use E.g. LP.py Network.txt 10"
        iterations = 5

    if len(argv)>3:
        THREADS = int(argv[3])
    if len(argv)>4:
        SizeHint = int(argv[4])
    else:
        print " No hint for Maximum number of nodes specified setting it to 25M"
        SizeHint=25000000
    return [filename,iterations,THREADS,SizeHint]

def LoadAdjDict(filename):
    Adj = AdjDict()
    data = open(filename)
    for entry in data:
        try:
            source = int(entry.rstrip('\n').split(' ')[0])
            target = int(entry.rstrip('\n').split(' ')[1])
        except:
            print " error while reading the file on line:", entry
        else:
           Adj[source].append(target)
    data.close()
    return Adj
