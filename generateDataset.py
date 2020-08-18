import random

# inputs: n (records), k (clusters), d (dimensions)
numPoints = [1000,10000,100000]
kClusters = [7,13]
dimPoints = [3,7]


for n in numPoints:
    for k in kClusters:
        for d in dimPoints:
            # open a new file
            f = open("data/data2_"+str(n)+"_"+str(k)+"_"+str(d)+".txt", "a")
            
            # compute the interval for creating the clusters
            interval = round(n/k)
            count = 0
            print("data2_"+str(n)+"_"+str(k)+"_"+str(d)+"; int: "+str(interval))
            
            # compute each point
            for i in range(n):
                if( (i%interval)==0 and i!=0):
                    count = count + 4
                
                x = ""
                for j in range(d):
                    x = x + str( interval*count + random.random()*interval )
                    if(j < d-1):
                        x = x + " "
                x = x + "\n"
                # write the new point coordinates in the file
                f.write(x)
            
            f.close()

