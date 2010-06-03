"""
Label Propagation algorithm using Multiprocessing module

Desc: Label Propagation algorithm using multiprocessing module

Parameters:
    1. Filename
    2. Number of iteration of the algorithm (default: 5)
    3. Number of processes to spawn (deafult: cpu_count())

Example Call: >>LP.py Net.txt 10 2 1000000

Input File format:
Each line contains a source node followed by a target node
both are integers, the delimeter used is a whitespace
E.g. 12 21\n

Repo:       http://github.com/AKSHAYUBHAT/Label-Propagation

Author:
Name:       Akshay Bhat
WebSite:    http://www.akshaybhat.com

"""
from lputil import AdjDict, maxVote, ParseOptions, LoadAdjDict, WriteFrequency, WriteMembership
import os, random, time, sys, array, logging, collections
from multiprocessing import Pool, cpu_count
from multiprocessing import Array as mpArray





                
if __name__ == '__main__':
    

    #Parse the Command line Options
    filename,iterations,THREADS,SizeHint=ParseOptions(sys.argv) # Parse the command line options

    # A shared array between multiple processes of type int. It is used since lookup for an array is O(1)
    # Also note that range automatically initializes the Label[key]=key
    # The lock is set to false since when data is written to Label only main process is active and pool is closed 
    # TO DO: This feature where Label is shared between multiple processes has not been impelemented yet
    Label = mpArray('i',range(25000000),lock=False)    


    # Load the Data in adjecancy list 
     
    start=time.time()
    Adj = LoadAdjDict(filename)     # Loads data to a AdjDict where each value is an array of integers since it allows faster acess
    print "\n Loaded the data in memmory.\n Time taken:",time.time()-start       
   

    #Enumerate all nodes 
    MapKeys = array.array('i')
    MapKeys.fromlist(Adj.keys())

    
    for iteration in range(1,iterations+1):
        pool = Pool(processes=THREADS)

        start=time.time()

        #Contains all result_async objects
        results = [] 
        
        # MapInput buffers the input to the processes 
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

        
        
        
        index = 0
        coms = collections.defaultdict(int);
        for result in results:
            for newLabel in result.get():
                Label[MapKeys[index]] = newLabel
                index+=1
                coms[newLabel]+=1

        print "Number of Communities:",len(coms)," iteration:",iteration  
        print "Time Taken: ",time.time()-start



        # Write the Community Frequency and Membership Information Result+filename directory        
        resultDir='Result'+filename.split('.')[0]
        try: 
            os.mkdir(resultDir)
        except:
            pass
        os.chdir(resultDir)
        WriteMembership("result"+str(iteration)+".txt",results,MapKeys)
        WriteFrequency("Communities"+str(iteration)+".txt" , coms)
        os.chdir('..')



