"""
Label Propagation algorithm using Multiprocessing 

Repo:       http://github.com/AKSHAYUBHAT/Label-Propagation

Name:       Akshay Bhat
WebSite:    http://www.akshaybhat.com



"""
import random, time, sys, array
from multiprocessing import Pool,Array,cpu_count

# Global Variable THREAD defines number of processes to be used
THREADS = cpu_count() 


def maxVote(nLabels):
    count = {}
    maxList = []
    maxCount = 0
    for nLabel in nLabels:
        if nLabel in count:
            count[nLabel] += 1
        else:
            count[nLabel] = 1
    #Check if the count is max
        if count[nLabel] > maxCount:
            maxCount = count[nLabel];
            maxList = [nLabel,]
        elif count[nLabel]==maxCount:
            maxList.append(nLabel)
    return random.choice(maxList)        
                


                
if __name__ == '__main__':
    Adj = {}      # A Python Dictionary since it allows faster acess

    #Parse the Command line Options
    if len(sys.argv)>1:
        filename = sys.argv[1] 
    else:
        filename = 'Network.txt'

    if len(sys.argv)>2:
        iterations = int(sys.argv[2]) 
    else:
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
            Label[source] = source  #initialize the label to itself 
            Label[target] = target  # just to be sure initialize the target as well
            if not(source in Adj):
                Adj[source] = [target,] #initialize the list
            else:
                Adj[source] += [target,]
            if not(target in Adj):
                Adj[target]=[]
    data.close()

    # Enumerate all nodes 
    MapKeys = Adj.keys()


    for iteration in range(1,iterations+1):
        start=time.time()

        # Prepare input for the Map
        MapInput = []
        random.shuffle(MapKeys)

        for key in MapKeys:
            temp=[]
            for target in Adj[key]:
                temp.append(Label[target])
            MapInput.append(temp)

        pool = Pool(processes=THREADS)
        result = pool.map(maxVote,MapInput)

        index=0;
        for newLabel in result:
            Label[MapKeys[index]]=newLabel
            index+=1
            
        print "Number of Communities:",len(set(result))," iteration:",iteration  
        print "Time Taken: ",time.time()-start

    pool.close()
