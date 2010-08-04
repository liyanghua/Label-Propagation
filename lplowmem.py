#!/usr/bin/env python
"""
Label Propagation algorithm using Multiprocessing module

Desc: Label Propagation algorithm using multiprocessing module

Parameters:
    1. Filename
    2. Number of iteration of the algorithm (default: 5)
    3. Number of processes to spawn (deafult: cpu_count())

Example Call: >>LP.py Net.txt 10 2 1000000

Input File format:
Each line contains a node followed by its neighbors delimeted by a whitespace.

Hack:
In order to share an array without creating extra copies and to use Pool a hideous
workaround is required to make it run on Windows
have a look at following stack overflow answer:
http://stackoverflow.com/questions/1675766/how-to-combine-pool-map-with-array-shared-memory-in-python-multiprocessing
The pmodule serves that purpose alone.


Repo:       http://github.com/AKSHAYUBHAT/Label-Propagation

Author:
Name:       Akshay Bhat
WebSite:    http://www.akshaybhat.com



"""

import os, random, time, sys, array, logging, collections
from multiprocessing import Pool, cpu_count, Array
import pmodule 

def ParseOptions(argv):
    """
    Parse the Command line Options
    """
    THREADS = cpu_count() # Variable THREAD defines number of processes to be used
    filename = ''
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
        SizeHint= 25 * 10**6
    return [filename,iterations,THREADS,SizeHint]


def ApplyAndVote(line,delim = ' '):
    """
    Applies the value of label for each neighbor
    and calls maxVote function
    """
    entry =  line.rstrip('\n').split(delim)
    node = int(entry[0])
    nLabels = [pmodule.Label[int(k)] for k in entry[1:]]
    return node, maxVote(nLabels)



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

def initProcess(Label):
    """
    initializes the process by setting the Label in pmodule
    """
    pmodule.Label = Label;


def WriteMembership(filename,results,delimeter = '\t'):
    """
    Writes membership of the communities as a tab delimited file
    """
    # write the Size of each community
    output=open(filename,'w')
    index=0
    for result in results:
        for entry in result.get():
            output.write(str(entry[0])+delimeter+str(entry[1])+'\n')
    output.close()



                
if __name__ == '__main__':
    #Parse the Command line Options
    filename,iterations,THREADS,SizeHint=ParseOptions(sys.argv) # Parse the command line options

    # A shared array between multiple processes of type int. It is used since lookup for an array is O(1)
    # Also note that range automatically initializes the Label[key]=key
    # The lock is set to false since when data is written to Label only main process is active and pool is closed 
    Label = Array('i',range(SizeHint),lock=False)    


    # Load the Data in adjecancy list 
    start = time.time()
    for iteration in range(1,iterations+1):
        iterstart = time.time()
        print "started iteration",iteration
        pool = Pool(processes=THREADS,initializer=initProcess,initargs=(Label,))

        Adj = open(filename)     # Open the file
        Buffer = []
        count  = 0
        #Contains all result_async objects
        results = []
        for line in Adj:
            Buffer.append(line)
            count += 1
            if count % 50000 == 0:
                if count % 10**6 == 0:
                    print count,time.time()-iterstart
                results.append(pool.map_async(ApplyAndVote,Buffer))
                del Buffer
                Buffer = []
        results.append(pool.map_async(ApplyAndVote,Buffer))
        del Buffer
        
        pool.close()
        pool.join()
        Adj.close()
        for result in results:
            for entry in result.get():
                Label[entry[0]] = entry[1]
        print "iteration finished time taken:",time.time()-iterstart
        iter2 = time.time()
        WriteMembership('data/res'+str(iteration)+'.txt',results)
        print "Finsied writing membership information to file:",time.time()-iter2


        
        


