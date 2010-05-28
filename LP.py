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
from lputil import AdjDict,maxVote,ParseOptions,LoadAdjDict
import random, time, sys, array, logging, collections
from multiprocessing import Pool, cpu_count



                
if __name__ == '__main__':
    

    #Parse the Command line Options
    filename,iterations,THREADS,SizeHint=ParseOptions(sys.argv) # Parse the command line options

    # An array of type int is used since lookup for an array is O(1)
    # Also note that range automatically initializes the Label[key]=key
    Label = array.array('i',range(25000000))    


    # Load the Data in adjecancy list 
     

    Adj = LoadAdjDict(filename)     # Loads data to a AdjDict where each value is an array of integers since it allows faster acess
           
   

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

