"""
[ NOT Ready Yet Please Do not Use it ]

Label Propagation algorithm using Multiprocessing module

Desc: This is a special version which does not loads the file into memmory
should be used with massive graphs such as wikipedia link network.
However the file format is different from the one used for LP.py


NOTE: There is a significant limitation to this program since it doen not randomizes the order in which
      nodes are processed
      
Parameters:
    1. Filename
    2. Number of iteration of the algorithm (default: 5)
    3. Number of processes to spawn (deafult: cpu_count())

Example Call: LPmem.py NetAdj.txt 10 2

Input File format:
Each line should start with a source node followed by all nodes linked to it,
The separator is a white space "  ".
All node identifiers have to be integers.
E.g.

Repo:       http://github.com/AKSHAYUBHAT/Label-Propagation

Author:
Name:       Akshay Bhat
WebSite:    http://www.akshaybhat.com

"""
import random, time, sys, array, logging, collections
from multiprocessing import Pool, cpu_count


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

    filename,iterations,THREADS,sizeHint=ParseOptions(sys.argv) # Parse the command line options

        
    Label = array.array('i',range(sizeHint))    # An array of type int is used since lookup for an array is O(1)

    for iteration in range(1,iterations+1):
        #Create a pool of processes
        pool = Pool(processes=THREADS)

        start=time.time()

        results = []
        MapBuffer = []
        MapKeys = []

        data = open(filename)
        try:
            for entry in data:
                temp = [];
                source = int(entry.rstrip('\n').split(' ')[0])
                targets = entry.rstrip('\n').split(' ')[1:]
                
                if len(targets)==0:
                    targets.append(source) # a minor hack such that nodes with no outlinks will have same label
                
                for target in targets:
                    temp.append(Label[int(target)])

                MapKeys.append(source)
                MapBuffer.append(temp)
                if len(MapBuffer)==30000:                
                    results.append(pool.map_async(maxVote,MapBuffer))
                    MapBuffer=[]
                    print "Map called"
        except:
            print " error while reading the file on line:", entry
            exit()
            
        data.close()
        pool.close()
        pool.join()
                                   
        index=0;

        output=open("result"+str(iteration)+".txt",'w')
        coms=[];
        for result in results:
            for newLabel in result.get():
                Label[MapKeys[index]]=newLabel
                index+=1
                coms.append(newLabel)
                output.write(str(MapKeys[index])+' '+str(newLabel)+'\n')
        output.close()
        
        print "Number of Communities:",len(set(coms))," iteration:",iteration  
        print "Time Taken: ",time.time()-start

    
