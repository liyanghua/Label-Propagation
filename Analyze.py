"""
A simple script to convert the node indentifiers to their names represented by file
Usage:
Analyze.py LabelFilename iteration directory
E.g. Analyze.py titles-sorted.txt 10 ResultNetwork
"""
import sys, os
from collections import defaultdict


if __name__ == '__main__':
    index=1;
    Label={}
    LabelFile = sys.argv[1]
    iteration = sys.argv[2]
    directory = sys.argv[3]

    data=open(LabelFile)
    for k in data:
        Label[index]=k.rstrip('\n')
        index+=1
    data.close()
    os.chdir(directory)
    Freq=open('Communities'+iteration+'.txt')
    Membership=open('result'+iteration+'.txt')
    out=open('Freq.txt','w')
    for k in Freq:
        k=k.rstrip('\n').split('\t')
        out.write(str(Label[int(k[0])])+'\t'+str(k[1])+'\n')
    out.close()

    out=open('Membership.txt','w')
    for k in Membership:
        k=k.rstrip('\n').split('\t')
        out.write(str(Label[int(k[0])])+'\t'+str(Label[int(k[1])])+'\n')
    out.close()
    Freq.close()
    Membership.close()
    os.chdir('..')
