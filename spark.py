import logging
import sys
import ConfigParser
from threading import Thread
import time
import os

#os.environ['SPARK_HOME'] = "/appl/spark/spark-spark2-2.0.0_scala_2.11"
#os.environ['PYTHONPATH']="/appl/spark/spark-spark2-2.0.0_scala_2.11/python:/appl/spark/spark-spark2-2.0.0_scala_2.11/python/lib/py4j-0.8.2.1-src.zip:$PYTHONPATH"
os.environ['SPARK_HOME'] = "/usr/lib/spark"
os.environ['PYTHONPATH']="/usr/lib/spark/python:/usr/lib/spark/python/lib/usr/lib/spark:$PYTHONPATH"

from pyspark.sql import HiveContext
from pyspark import SparkContext, SparkConf


logging.basicConfig(stream=sys.stdout, level=logging.INFO)

LOG = logging.getLogger(__name__)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        logging.fatal("Usage: <%s> <config file>" , sys.argv[0])
        sys.exit(-1)

    Config = ConfigParser.ConfigParser()
    Config.read(sys.argv[1])
    spark_location =  Config.get("connectioninfo","spark_location")
    spark_version =  Config.get("connectioninfo","spark_version")
    spark_executor_num =  Config.get("connectioninfo","spark_executor_num")
    spark_executor_mem =  Config.get("connectioninfo","spark_executor_mem")
    spark_executor_cores =  Config.get("connectioninfo","spark_executor_cores")

    suitList = Config.get("testinfo","suits").split(",")
    iterations = Config.getint("testinfo","iterations")
    results = Config.get("testinfo", "resultsfile")
    concurrency = Config.getint("testinfo","concurrency")

    conf = SparkConf()
    conf.setAppName("Spark SQL Driver")
    conf.set("spark.executor.instances", spark_executor_num )
    sc = SparkContext (conf=conf)

    sqlContext = HiveContext(sc)
    result = sqlContext.sql ("select * from customers").collect()
    #sqlContext.sql ("show tables")
    print (result)



