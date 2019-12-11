import sys
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import Row
import time


#### Driver program

# start spark with 1 worker thread
sc = SparkContext("local[1]")
spark = SparkSession(sc)
sc.setLogLevel("ERROR")


# read the input file into an RDD[String]
wholeFile = sc.textFile("./data/machine_events/part-00000-of-00001.csv")

# parse file into dataframe
csvDF = wholeFile\
    .map(lambda x: Row(timestamp = str(x.split(",")[0]),\
                       machine_id = int(x.split(",")[1]),\
                       event_type = str(x.split(",")[2]),\
					   plateform_id = str(x.split(",")[3]),\
					   cpu_capacity = float(x.split(",")[4]),\
					   mem_capacity = float(x.split(",")[5]))).toDF()



csvDF.show(100)




#print(wholeFile.collect())

## OTHER WAY TO PARSE DATAFILE
# split each line into an array of items
#entries = wholeFile.map(lambda x : x.split(','))

# keep the RDD in memory
#entries.cache()

#print(entries.collect())
