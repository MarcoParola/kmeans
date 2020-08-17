import sys
from random import random
from operator import add

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
        
    return dist



def findCenter(point, centers):
    minDist = float("inf")
    for i in range(len(centers)):
        newDist = computeDistance(point, centers[i])
        # if newDist < 0: gestire errore
        if (newDist < minDist):
            minDist = newDist
            minIndex = i
    return minIndex


"""
def oldComputeNewCenter(points, numPoints, d):
    for i in range(d):
        newCenter[i] = float(0)
    
    for xx in points:
        x = xx.split(" ")
        for i in range(d):
            newCenter[i] = newCenter[i] + float(x[i])
    
    for i in range(d):
        newCenter[i] = newCenter[i] / float(numPoints)
    
    return newCenter
"""     

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


def check(centers, newCenters):
    dist = 0.0
    
    for i in range(len(centers)):
        dist = dist + computeDistance(centers[i], newCenters[i])
    
    return dist


if __name__ == "__main__":
    if len(sys.argv) < 5:
        print("Usage: wordcount <input file> <numPoints> <numDimensions> <numClusters> [<output file>]", file=sys.stderr)
        sys.exit(-1)

    master = "yarn"
    sc = SparkContext(master, "KMeans")
    
    MAX_ITERATIONS = 10
    THRESHOLD = 0.5
    
    # saving number of points (n), number of dimensions (d), and number of clusters (k)
    n = int(sys.argv[2])
    d = int(sys.argv[3])
    k = int(sys.argv[4])
    
    # reading the input points
    points = sc.textFile(sys.argv[1])
    
    # initializing centers (with the action 'takeSample' without replacement)
    # return an array, not an RDD
    centers = points.takeSample(False, k, round(random()*n))
    
    
    for iteration in range(MAX_ITERATIONS):
        
        # printing the current centers
        for i in range(len(centers)):
            print("_____ CENTER "+str(i)+": "+centers[i]+"_____")
        
        ## SETTING THE SPARK STEPS (transformations and actions)
        # mapping to pairs (key: point (int), value: center_index (str))
        clusters = points.map(lambda point: (findCenter(point, centers), point)).sortByKey()
        # getting an array with each cluster's dimension
        dimClusters = clusters.countByKey();
        for i in range(len(dimClusters)):
            print("_____ DIM CLUSTER "+str(i)+": "+ str(dimClusters[i])+"_____")
        # computing new centers with 'reduceByKey' transformation
        newCenters = clusters.reduceByKey(computeNewCenter).collect()
        
        # saving the new centers as array
        arrNewCenters = []
        for cc in newCenters:
            arrNewCenters.append(cc[1])
            print("_____ CENTER: "+str(cc)+"_____")
        
        # checking the stop condition
        if check(centers, arrNewCenters) < THRESHOLD:
            break
        else:
            centers = arrNewCenters
    
    
    if len(sys.argv) == 6:
        newCenters.repartition(1).saveAsTextFile(sys.argv[2])
    else:
        for cc in newCenters:
            print("_____ CENTER "+str(i)+": "+cc+"_____")

