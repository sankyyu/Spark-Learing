from pyspark import SparkConf,SparkContext

conf=SparkConf().setMaster("local").setAppName("totalAmount")
sc=SparkContext(conf=conf)

def parseLine(line):
	fields=line.split(',')
	return (int(fields[0]),float(fields[2]))

lines=sc.textFile("./customer-orders.csv")
eachCustomer=lines.map(parseLine)
amount=eachCustomer.reduceByKey(lambda x,y:x+y)
amount=amount.map(lambda x:(x[1],x[0]))
results=amount.sortByKey().collect()

for result in results:
	print (result)

