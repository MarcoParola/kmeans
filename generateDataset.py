import random

# inputs: n (records), k (clusters), d (dimensions)
numPoints = [1000,10000,100000]
kClusters = [7,13]
dimPoints = [3,7]


for n in numPoints:
    for k in kClusters:
        for d in dimPoints:
            # open a new file
            f = open("data/dataset_"+str(n)+"_"+str(k)+"_"+str(d)+".txt", "a")
            
            # compute the interval for creating the clusters
            interval = round(n/(2*k))
            count = 0
            print("dataset_"+str(n)+"_"+str(k)+"_"+str(d)+"; int: "+str(interval))
            
            # compute each point
            for i in range(n):
                if( (i%interval)==0 and i!=0):
                    count = count + 2
                
                x = ""
                for j in range(d):
                    x = x + str( interval*count + random.random()*interval )
                    x = x + " "
                x = x + "\n"
                # write the new point coordinates in the file
                f.write(x)
            
            f.close()

# 1000 punti, 7 clusters -> n/(2k)
# 1 int: 0 - n/2k
# 2 int: 2*n/2k - 3*n/2k
# 3 int: 4*n/2k - 5*n/2k
