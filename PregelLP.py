"""
A Pregel like implementation of Label Propagation based Community Detection Algorithm

Parameters:
    1. Filename
    2. Number of iteration of the algorithm (default: 5)
    3. Number of Worker processes to spawn (deafult: cpu_count())

Example Call: >>LP.py Net.txt 10 2 1000000

Input File format:
Each line contains a source node followed by a target node
both are integers, the delimeter used is a whitespace
E.g. 1 2 3 4 5 6\n

Repo:       http://github.com/AKSHAYUBHAT/Label-Propagation

Author:
Name:       Akshay Bhat
WebSite:    http://www.akshaybhat.com

"""
from lputil import AdjDict, maxVote, ParseOptions, LoadAdjDict, WriteFrequency, WriteMembership
import os, random, time, sys, array, logging, collections
from multiprocessing import Pool, cpu_count, Manager
from multiprocessing import Array as mpArray

def Compute(node,AdjNode,MsgList):
    nLabels = [k[1] for k in MsgList] 
    newLabel = maxVote(nLabels)
    return [(node,k,newLabel) for k in AdjNodes]

def Worker(Compute,NodeRange,Adj,CurMsg):
    result=[]
    for node in NodeRange:
        result+=Compute(node,Adj[node],CurMsg[node])
    return result    
    

def Master(Steps,numWorkers,Compute,Adj,InitMsg):
    CurMsg = InitMsg
    Nodes = Adj.keys()
    temp = range(0,len(Nodes)+1,len(Nodes)/numWorkers)
    NodeRange = []
    for k in range(numWorkers):
        NodeRange.append(Nodes[temp[k]:temp[k+1]])
    NodeRange[-1].append(Nodes[-1])
    for S in range(Steps):
        Workers = Pool(numWorkers)
        Results = []
        for W in range(numWorkers):
            Results.append(Workers.apply_async(Worker,(Compute,NodeRange[W],Adj,CurMsg)))
        Workers.close()
        Workers.join()
        del CurMsg
        CurMsg = Manager().dict()
        for result in Results:
            for Message in result.get():
                Source = Message[0]
                Recipient = Messag[1]
                Body = Message[2]
                try:
                    CurMsg[Recipient]+=[(source,body),]
                except:
                    CurMsg[Recipient]=[(source,body),]
        


                
if __name__ == '__main__':
    MainManager = Manager()
    Adj = MainManager.dict()
    InitMsg = MainManager.dict()
    data = open('NetworkMem.txt')
    for line in data:
        line= [ int(k) for k in line.rstrip('\n').split(' ')]
        Adj[line[0]]=array.array('i')
        Adj[line[0]].fromlist(line[1:])
        InitMsg[line[0]]=array.array('i').fromlist([line[0],line[0],line[0]])
    print 'Master Started'
    Master(2,2,Compute,Adj,InitMsg)



