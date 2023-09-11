from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("CustomerCount")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    id = fields[0]
    spent = float(fields[2])
    return (id, spent)

lines = sc.textFile("customer-orders.csv")
parsedLines = lines.map(parseLine)
spent = parsedLines.reduceByKey(lambda a,b: a+b).mapValues(lambda x:round(x,2))
spentSorted = spent.map(lambda x:(x[1] , x[0])).sortByKey(False)

results = spentSorted.collect() #.collect()

for result in results:
    print(f"{result[1]}: {result[0]}" )