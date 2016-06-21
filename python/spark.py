import logging
import sys
import ConfigParser
from threading import Thread
import time
import os



logging.basicConfig(stream=sys.stdout, level=logging.INFO)

LOG = logging.getLogger(__name__)

def suiterun (tname,ctx,version,suits,run,r):
    # Open Impala Connection
    logging.info ("Running %s", tname)

    # chose impala daemon randomly from the list
   # subscript = randint(0,len(h)-1)
   # h = h[subscript]
    conns = []
    try:
        while run > 0:
            run = run  -1
            for i in suits:
                topdir = "/appl/perfbench/latest/suites/" + i + "/spark"+ version
                for root,dirs,files  in os.walk(topdir,topdown="False"):
                    for name in files:
                        with open(os.path.join(root, name),'r') as queryfile:
                            query = queryfile.read()
                            now = time.time()
                            # execute query
                            results = ctx.sql (query).collect ()
                            ############ close cursor
                            duration  = time.time() - now
                            # Append the result to file
                            f = open (r,'a')
                            resultline = "spark"+version+","+tname+","+i+","+name + "," +  run.__str__() + "," + duration.__str__()+"\n"
                            f.write(resultline)
                            f.close()
    except Exception, e:
        logging.error("Could not execute: " + e.__str__())


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


    os.environ['SPARK_HOME'] = spark_location
    os.environ['PYTHONPATH'] = spark_location + "/python:"+ spark_location + "/python/lib/usr/lib/spark:$PYTHONPATH"
    os.environ['HADOOP_CONF_DIR'] = "/etc/hadoop/conf"
    os.environ['YARN_CONF_DIR'] = "/etc/hadoop/conf"
    sys.path.append(spark_location + "/python")


    from pyspark.sql import HiveContext
    from pyspark import SparkContext, SparkConf

    if spark_version == "2.0":
        try:
            from pyspark.sql import SparkSession
            sqlContext = SparkSession.builder.master("yarn").enableHiveSupport().getOrCreate ()
        except:
            logging.warn ("This version of Spark is not 2.0 so not doing SparkSessions")
    else:
        conf = SparkConf()
        conf.setAppName("Spark SQL Driver")
        conf.set("spark.executor.instances", spark_executor_num )
        sc = SparkContext (conf=conf)
        sqlContext = HiveContext(sc)


    threads = []
    while concurrency > 0:
        name = "Thread #" + concurrency.__str__()
        try:
            thread = Thread (target=suiterun,args = (name,sqlContext,spark_version,suitList,iterations,results))
            threads.append(thread)
            #thread.start_new_thread(suiterun,(name,host,port,suitList,iterations,results,))
        except:
            logging.fatal("Unable to start thread %s", name)
        concurrency = concurrency-1
    for t in threads:
        t.start()
    for t in threads:
        t.join()




