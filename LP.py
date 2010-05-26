"""
Label Propagation algorithm using Multiprocessing 

Repo:       http://github.com/AKSHAYUBHAT/Label-Propagation

Name:       Akshay Bhat
WebSite:    http://www.akshaybhat.com



"""
import random, time, sys, array, logging, collections
from multiprocessing import Pool, cpu_count

# Global Variable THREAD defines number of processes to be used
THREADS = cpu_count() 



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
    cnt = collections.defaultdict(int)
    if nLabels==[]:
        return None
    for i in nLabels:
        cnt[i] += 1
    maxv = max(cnt.itervalues())
    return random.choice([k for k,v in cnt.iteritems() if v == maxv])



                
if __name__ == '__main__':
    Adj = AdjDict()     # A Python Dictionary since it allows faster acess

    #Parse the Command line Options
    # filename
    if len(sys.argv)>1:
        filename = sys.argv[1] 
    else:
        'Please specify file name E.g. LP.py Network.txt'

    if len(sys.argv)>2:
        iterations = int(sys.argv[2]) 
    else:
        print " Number of Iterations not specified, using 5 iterations "
        print " specify iterations use E.g. LP.py Network.txt 10"
        iterations = 5

        
    Label = array.array('i',range(1000000))    # An array of type int is used since lookup for an array is O(1)

    # Load the Data in adjecancy list and initialize labels
     

    data = open(filename)
    for entry in data:
        try:
            source = int(entry.rstrip('\n').split(' ')[0])
            target = int(entry.rstrip('\n').split(' ')[1])
        except:
            print " error while reading the file on line:", entry
        else:
            Label[source] = source  # initialize the label to itself 
            Label[target] = target  # just to be sure initialize the target as well
            Adj[source].append(target)

            
    data.close()

    #Enumerate all nodes 
    MapKeys = Adj.keys()

    pool = Pool(processes=THREADS)
    for iteration in range(1,iterations+1):
        start=time.time()

        # Prepare input for the Map
        MapInput = []
        random.shuffle(MapKeys)

        for key in MapKeys:
            temp=array.array('i')
            for target in Adj[key]:
                temp.append(Label[target])
            MapInput.append(temp)

        result=[]
        result = pool.map(maxVote,MapInput)

        index=0;
        for newLabel in result:
            Label[MapKeys[index]]=newLabel
            index+=1
            
        print "Number of Communities:",len(set(result))," iteration:",iteration  
        print "Time Taken: ",time.time()-start

    #pool.close()
