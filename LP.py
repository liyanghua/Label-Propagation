"""
Name: Akshay Bhat




"""
import multiprocessing, random, time

THREADS = 2 
ITERATIONS = 5
LabelNew={};




def Propagate(start,end,LabelP,AdjP,MapListP):
    Res={};
    print "running Thread with range: ",start,end
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
                
    for num in range(start,end):
        node=MapListP[num]
        nLabels = [LabelP[k] for k in AdjP[node]]
        if (nLabels!=[]):
            Res[node] = maxVote(nLabels)
        else:
            Res[node]=LabelP[node]
    print "finished a worker"
    return Res

                
if __name__ == '__main__':
    Label = {}    # A Dictionary for storing labels of current node
    Adj = {}      # A Python Dictionary since it allows faster acess
    MapList =[]   # a list randomely ordered 


    # Load the Data in adjecancy list and initialize labels
     

    data = open("Network.txt")
    for entry in data:
        source = entry.rstrip('\n').split(' ')[0]
        target = entry.rstrip('\n').split(' ')[1]
        Label[source] = source  #initialize the label to itself 
        Label[target] = target  # just to be sure initialize the target as well
        if not(source in Adj):
            Adj[source] = [target,] #initialize the list
        else:
            Adj[source] += [target,]
        if not(target in Adj):
            Adj[target]=[]
    data.close()

    #Create MapList
    MapList = Adj.keys()


    # used to determine which range of nodes to be assigned to which thread
    MapRange=range(0,len(MapList)+1,len(MapList)/THREADS)

    MapRange[-1]=len(MapList)

    for k in range(ITERATIONS):
        random.shuffle(MapList)
        args=[];
        print "starting pool"
        pool = multiprocessing.Pool(processes=THREADS)
        result=[]
        for k in range(THREADS):
            args.append((MapRange[k],MapRange[k+1],Label,Adj.copy(),MapList))
        for k in range(THREADS):
            print "created an arg"
            result.append(pool.apply_async(Propagate,args[k]))
            print "applied a process"
        print "started threads"
        pool.close();
        print "close called"
        pool.join();
        print "join called"
        res=[]
        count=[]
        for k in result:
            res.append(k.get())

        for process in res:
            for entry in process:
                Label[entry]=process[entry];
                count.append(process[entry])
        print len(set(count))
        
    print "done"

