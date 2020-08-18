import math
import sys
from datetime import datetime
from random import random

from pyspark import SparkContext


class Sample:
    
    def __init__(self, coord, weight):
        self.weight = int(weight)
        self.coord = str(coord)
        
    def getCoord(self):
        return self.coord
            
    def getCoordAsArray(self):
        return self.coord.split(" ")
    
    def getWeight(self):
        return self.weight
    
    def toString(self):
        return self.coord + "\t" + str(self.weight)


def is_float(s):
    try:
        float(s)
        return True
    except ValueError:
        return False


def sampleFromString(sample):
    param = sample.split("\t")
    if len(param) < 2:
        weight = 1
    else:
        weight = param[1]
    return Sample(param[0],weight)


# computing distance through Euclidian norm
def computeDistance(pointX, pointY):
    xx = pointX.split(" ")
    yy = pointY.split(" ")
    
    if( len(xx) != len(yy) ):
        return 0
    
    dist = float(0)
    for i in range(len(xx)):
        if is_float(xx[i]) and is_float(yy[i]):
            x = float(xx[i])
            y = float(yy[i])
            dist = dist + (x - y)*(x - y)
        
    return math.sqrt(dist)


def findCenter(point, centers):
    minDist = float("inf")
    for i in range(len(centers)):
        newDist = computeDistance(point, centers[i])
        # if newDist < 0: gestire errore
        if (newDist < minDist):
            minDist = newDist
            minIndex = i
    return minIndex


def computeNewCenter(pointX, pointY):
    print("computeNewCenter: "+pointX+", "+pointY)
    
    newCenterCoord = ""
    sampleX = sampleFromString(str(pointX))
    sampleY = sampleFromString(str(pointY))
    newCenterWeight = sampleX.getWeight() + sampleY.getWeight()
    xx = sampleX.getCoordAsArray()
    yy = sampleY.getCoordAsArray()
    if len(xx) != len(yy):
        return 0
    
    for i in range(len(xx)):
        if is_float(xx[i]) and is_float(yy[i]):
            x = float(xx[i])*sampleX.getWeight()
            y = float(yy[i])*sampleY.getWeight()
            newCenterCoord = newCenterCoord + str((x+y)/newCenterWeight) + " "
    
    return Sample(newCenterCoord, newCenterWeight).toString()


def checkCenters(centers, newCenters):
    dist = 0.0
    
    for i in range(len(centers)):
        dist = dist + computeDistance(centers[i],newCenters[i])
    
    return dist



if __name__ == "__main__":
    if len(sys.argv) < 6:
        print("Usage: kmeans <input file> <numPoints> <numDimensions> <numClusters> <numPartitions> [<output file>]", file=sys.stderr)
        sys.exit(-1)

    master = "yarn"
    sc = SparkContext(master, "KMeans")
    
    MAX_ITERATIONS = 100
    THRESHOLD = 0.5
    
    # saving number of points (n), number of clusters (k), number of dimensions (d), and number of partitions
    n = int(sys.argv[2])
    k = int(sys.argv[3])
    d = int(sys.argv[4])
    partitions = int(sys.argv[5])
    
    
    STARTING_TIME = datetime.now()
    # reading the input points
    points = sc.textFile(sys.argv[1])
    
    # initializing centers (with the action 'takeSample' without replacement)
    # return an array, not an RDD
    centers = points.takeSample(False, k, 34231)#round(random()*n)))

    # starting algorithm
    for iteration in range(MAX_ITERATIONS):
        print("_____ ITERATION "+str(iteration+1)+" _____\n")
        
        for i in range(len(centers)):
            print("_____ CENTER "+str(i)+": "+centers[i]+"\n")
        
        ## SETTING THE SPARK STEPS (transformations and actions)
        # mapping to pairs (key: center_index (int), value: coord + weight (str))
        clusters = points.map(lambda point: ( findCenter(point,centers), Sample(point,1).toString())).sortByKey()
        
        # computing new centers with 'reduceByKey' transformation
        newCenters = clusters.reduceByKey(computeNewCenter,partitions).sortByKey()
        
        # saving the new centers as array
        arrNewCenters = []
        for cc in newCenters.collect():
            arrNewCenters.append(sampleFromString(cc[1]).getCoord())
        
        # checking the stop condition
        diff = checkCenters(centers, arrNewCenters)
        if diff < THRESHOLD:
            break
        else:
            centers = arrNewCenters
    # ending algorithm
    
    ENDING_TIME = datetime.now()
    
    # saving info in local file
    outputFile = open("out_spark/output","a")
    outputFile.write(sys.argv[1]+"\n")
    outputFile.write("time: "+str(ENDING_TIME - STARTING_TIME)+", iterations: "+str(iteration+1)+", partitions: "+str(partitions)+"\n")
    # printing the centers
    for cc in newCenters.collect():
        c =  sampleFromString(cc[1])
        outputFile.write(str(cc[0])+"\t"+c.getCoord()+" clusterDim: "+str(c.getWeight())+"\n")
    outputFile.write("------------------------------------------------------------------------------------------\n")
    outputFile.close()
    
    # saving results in hdfs
    if len(sys.argv) == 7:
        newCenters.repartition(1).saveAsTextFile(sys.argv[6]+"_"+str(n)+"_"+str(k)+"_"+str(d))
    else:
        for cc in newCenters.collect():
            print("_____ CENTER: "+str(cc)+"_____")

