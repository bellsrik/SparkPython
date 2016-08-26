from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("fakefriends")
sc = SparkContext(conf = conf)

def parseLine(x):
    fields = x.split(',')
    name = fields[1]
    numFriends = int(fields[3])
    return (name, numFriends)

lines = sc.textFile("file:///SparkPython/fakefriends.csv")
rdd = lines.map(parseLine)

totalByName = rdd.mapValues(lambda x: (x,1)).reduceByKey(lambda x,y: (x[0] + y [0], x[1] + y[1]))
averagesByName = totalByName.mapValues(lambda x: x[0]/x[1])
results = averagesByName.collect()
for result in results:
    print result
    



