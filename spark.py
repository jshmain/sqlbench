import logging
import sys
import ConfigParser
from threading import Thread
import time
import os

os.environ['SPARK_HOME'] = "/appl/spark/spark-2.0.0-preview-bin-hadoop2.6"
os.environ['PYTHONPATH']="/appl/spark/spark-2.0.0-preview-bin-hadoop2.6/python:/appl/spark/spark-2.0.0-preview-bin-hadoop2.6/python/lib/py4j-0.8.2.1-src.zip:$PYTHONPATH"
#os.environ['SPARK_HOME'] = "/usr/lib/spark"
#os.environ['PYTHONPATH']="/usr/lib/spark/python:/usr/lib/spark/python/lib/usr/lib/spark:$PYTHONPATH"

from pyspark.sql import HiveContext
from pyspark import SparkContext, SparkConf


logging.basicConfig(stream=sys.stdout, level=logging.INFO)

LOG = logging.getLogger(__name__)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        logging.fatal("Usage: <%s> <config file>" , sys.argv[0])
        sys.exit(-1)

    sc = SparkContext ()
    sqlContext = HiveContext(sc)
    #result = sqlContext.sql ("select * from customers").collect()
    #print (result)



