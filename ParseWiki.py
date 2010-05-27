"""
A simple script to parse wikipedia link file.
The Wikpedia link dataset is taken
from http://users.on.net/~henry/home/wikipedia.htm
"""
import sys



if __name__ == '__main__':
    data=open('links-simple-sorted.txt')
    if sys.argv[1]=='List':
        out=open("Network.txt",'w')
        for line in data:
            source=line.split(':')[0]
            targets=line.split(':')[1].rstrip('\n').lstrip(' ').split(' ')
            for target in targets:
                if (target!='' and target!=' '): 
                    out.write(source+' '+target+'\n')
        out.close()
    if sys.argv[1]=='Adj':
        out=open("NetworkMem.txt",'w')
        for line in data:
            out.write(line.replace(':',''))
        out.close()
    data.close()
