"""
Label Propagation algorithm using Multiprocessing module

Desc: Label Propagation algorithm using multiprocessing module

Parameters:
    1. Filename
    2. Number of iteration of the algorithm (default: 5)
    3. Number of processes to spawn (deafult: cpu_count())

Example Call: >>LP.py NetAdj.txt 10 2

Input File format:
Each line contains a source node followed by a target node
both are integers, the delimeter used is a whitespace
E.g. 12 21\n

Repo:       http://github.com/AKSHAYUBHAT/Label-Propagation

Author:
Name:       Akshay Bhat
WebSite:    http://www.akshaybhat.com

"""
import random, time, sys, array, logging, collections
from multiprocessing import Pool, cpu_count





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
                
if __name__ == '__main__':
    Adj = AdjDict()     # A Python Dictionary since it allows faster acess

    #Parse the Command line Options
    filename,iterations,THREADS,SizeHint=ParseOptions(sys.argv) # Parse the command line options

    Label = array.array('i',range(25000000))    # An array of type int is used since lookup for an array is O(1)

    # Load the Data in adjecancy list and initialize labels
     

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

    #Enumerate all nodes 
    MapKeys = array.array('i')
    MapKeys.fromlist(Adj.keys())

    
    for iteration in range(1,iterations+1):
        pool = Pool(processes=THREADS)
        start=time.time()

        results = [] # contains result_async objects
        
        # MapInput stores the buffer
        MapInput = []
        
        random.shuffle(MapKeys)

        for key in MapKeys:
            temp = [];
            for target in Adj[key]:
                temp.append(Label[target])
            MapInput.append(temp)
            if len(MapInput)==100000:
                results.append(pool.map_async(maxVote,MapInput))
                MapInput=[]
        pool.close()
        pool.join()
        
        index=0
        # write the Community membership information
        output=open("result"+str(iteration)+".txt",'w')
        coms=deafaultdict(int);
        for result in results:
            for newLabel in result.get():
                Label[MapKeys[index]]=newLabel
                index+=1
                coms[newLabel]+=1
                output.write(str(MapKeys[index])+' '+str(newLabel)+'\n')
        output.close()

        # write the Size of each community
        output=open("Communities"+str(iteration)+".txt",'w')
        for k in coms:
            output.write(k+'\t'+str(coms[k])+'\n')
        output.close()
            
        print "Number of Communities:",len(coms)," iteration:",iteration  
        print "Time Taken: ",time.time()-start

