#standard spark python libraries to be imported
from pyspark import SparkConf, SparkContext
#import collections

#Spark Stuff to set up spark conf and spark context
conf = SparkConf().setMaster("local").setAppName("Total Amount Spent")
sc = SparkContext(conf = conf)

#function to split fields and assign values
def parseLine(xyz):
    fields = xyz.split(',')
    cust_id = int(fields[0])
    dollar_amt = float(fields[2])
    return (cust_id, dollar_amt)
  
#core of the application  
lines = sc.textFile("file:///SparkPython/customer-orders.csv")
custAmtSpent = lines.map(parseLine)
custTotalSpent = custAmtSpent.reduceByKey(lambda x, y: x + y)
formatedList = custTotalSpent.mapValues(lambda x: ("{:.2f}".format(x)))
flipped = formatedList.map(lambda x: (x[1], x[0])).sortByKey()
results = flipped.collect()

#print preference
for result in results:
    print(result) 