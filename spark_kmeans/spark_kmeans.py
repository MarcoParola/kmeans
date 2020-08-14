import sys
from random import random
from operator import add

from pyspark import SparkContext

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: wordcount <input file> [<output file>]", file=sys.stderr)
        sys.exit(-1)

    master = "yarn"
    sc = SparkContext(master, "KMeans")

    # reading the input points
    points = sc.textFile(sys.argv[1])
    # saving number of points (n), number of dimensions (d), and number of clusters (k)
    values = sys.argv[1].split("_")
    if len(values) < 4:
        print("Invalid input file")
        sys.exit(-1)
    n = values[1]
    d = values[2]
    k = values[3]
    
    # initializing centers (with the action 'takeSample' without replacement)
    # return an array, not an RDD
    centers = points.takeSample(False, k, random())
    
    # setting the spark steps (transformations and actions)
    clusters = points.keyBy(lambda xx: findCenter(xx, centers))
    dimClusters = clusters.groupByKey().count().collect();
    newCenters = clusters.reduceByKey(lambda values, key: computeNewCenter(values, dimClusters[key],centers[key]))
        
    
    
    if len(sys.argv) == 3:
        newCenters.repartition(1).saveAsTextFile(sys.argv[2])
    else:
        output = newCenters.collect()
        for (center) in output:
            print("center: %i" % (center))



def computeDistance(pointX, pointY):
    xx = pointX.split(" ")
    yy = pointY.split(" ")
    
    if( len(xx) != len(yy) )
        return -1
    
    dist = 0
    for i in range(len(xx)):
        dist = dist + (float(xx[i]) - float(yy[i]))*float(xx[i]) - float(yy[i]))
        
    return dist



def findCenter(point, centers):
    minDist = float("inf")
    for i in range(len(centers)):
        newDist = computeDistance(point, centers[i])
        if (newDist < minDist):
            minDist = newDist
            minIndex = i
    return minIndex



def computeNewCenter(points, numPoints, d):
    for i in range(d):
        newCenter[i] = float(0)
    
    for xx in points:
        x = xx.split(" ")
        for i in range(d):
            newCenter[i] = newCenter[i] + x[i]
    
    for i in range(d):
        newCenter[i] = newCenter[i] / numPoints
    
    return newCenter
        
    

