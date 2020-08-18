import math
import sys
from datetime import datetime
from random import random

from pyspark import SparkContext

def is_float(s):
    try:
        float(s)
        return True
    except ValueError:
        return False


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
    newCenter = ""
    xx = pointX.split(" ")
    yy = pointY.split(" ")
    if len(xx) != len(yy):
        return 0
    
    for i in range(len(xx)):
        if is_float(xx[i]) and is_float(yy[i]):
            x = float(xx[i])
            y = float(yy[i])
            newCenter = newCenter + str((x+y)/2.0) + " "
    
    return newCenter


def checkDifference(centers, newCenters):
    dist = 0.0
    
    for i in range(len(centers)):
        dist = dist + computeDistance(centers[i], newCenters[i])
    
    return dist


if __name__ == "__main__":
    if len(sys.argv) < 6:
        print("Usage: kmeans <input file> <numPoints> <numDimensions> <numClusters> <numPartitions> [<output file>]", file=sys.stderr)
        sys.exit(-1)

    master = "yarn"
    sc = SparkContext(master, "KMeans")
    
    MAX_ITERATIONS = 30
    THRESHOLD = 0.5
    
    
    # saving number of points (n), number of dimensions (d), and number of clusters (k)
    n = int(sys.argv[2])
    d = int(sys.argv[3])
    k = int(sys.argv[4])
    partitions = int(sys.argv[5])
    
    
    STARTING_TIME = datetime.now()
    # reading the input points
    points = sc.textFile(sys.argv[1])
    
    # initializing centers (with the action 'takeSample' without replacement)
    # return an array, not an RDD
    centers = points.takeSample(False, k, 34231)#round(random()*n)))
    
    
    outputFile = open("output.txt","a")
    outputFile.write("Input: "+sys.argv[1]+", partitions: "+str(partitions)+"\n\n")
    
    for iteration in range(MAX_ITERATIONS):
        outputFile.write("_____ ITERATION "+str(iteration+1)+" _____\n")
        # printing the current centers
        for i in range(len(centers)):
            outputFile.write("_____ CENTER "+str(i)+": "+centers[i]+"\n")
        
        ## SETTING THE SPARK STEPS (transformations and actions)
        # mapping to pairs (key: point (int), value: center_index (str))
        clusters = points.map(lambda point: (findCenter(point, centers), point)).sortByKey()
        # getting an array with each cluster's dimension
        dimClusters = clusters.countByKey();
        for i in range(len(dimClusters)):
            outputFile.write("_____ DIM CLUSTER "+str(i)+": "+ str(dimClusters[i])+"\n")
        # computing new centers with 'reduceByKey' transformation
        newCenters = clusters.reduceByKey(computeNewCenter,partitions).sortByKey().collect()
        
        # saving the new centers as array
        arrNewCenters = []
        for cc in newCenters:
            arrNewCenters.append(cc[1])
        
        # checking the stop condition
        diff = checkDifference(centers, arrNewCenters)
        outputFile.write("_____ Difference between old and new centers: "+str(diff)+" _____\n\n\n")
        if diff < THRESHOLD:
            break
        else:
            centers = arrNewCenters
    
    ENDING_TIME = datetime.now()
    outputFile.write("Start: "+str(STARTING_TIME)+", end: "+str(ENDING_TIME)+", time: "+str(ENDING_TIME - STARTING_TIME)+"\n")
    outputFile.write("------------------------------------------------------------------------------------------\n")
    outputFile.close()
    
    if len(sys.argv) == 7:
        newCenters.repartition(1).saveAsTextFile(sys.argv[2])
    else:
        for cc in newCenters:
            print("_____ CENTER: "+str(cc)+"_____")

