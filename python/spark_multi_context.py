import logging
import sys
import ConfigParser
import time
import os
import subprocess


logging.basicConfig(stream=sys.stdout, level=logging.INFO)

LOG = logging.getLogger(__name__)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        logging.fatal("Usage: <%s> <config file>" , sys.argv[0])
        sys.exit(-1)

    Config = ConfigParser.ConfigParser()
    Config.read(sys.argv[1])

    concurrency = Config.getint("testinfo","concurrency")
    launch_script  = os.path.dirname (sys.argv[0]) + "/launch_spark_client.py"
    threads = []
    while concurrency > 0:
      name = "Thread_" + concurrency.__str__()
      p = subprocess.Popen (["python", launch_script , name, sys.argv[1]])
      threads.append(p)
      concurrency = concurrency-1
    for t in threads:
        t.wait()
        
    logging.info ("Spark All Done.")
