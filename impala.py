#!/usr/bin/python

from impala.dbapi import connect
import logging
import sys
import ConfigParser
from threading import Thread
import time
import os


logging.basicConfig(stream=sys.stdout, level=logging.INFO)

LOG = logging.getLogger(__name__)

def suiterun (tname,h,p,suits,run,r):
    # Open Impala Connection
    logging.info ("Running %s", tname)
    try:
        conn = connect(host=h, port=p)
        cursor = conn.cursor()
        while run > 0:
            run = run  -1
            for i in suits:
                topdir = "/appl/perfbench/latest/suits/" + i + "/impala"
                for root,dirs,files  in os.walk(topdir,topdown="False"):
                    for name in files:
                        with open(os.path.join(root, name),'r') as queryfile:
                            query = queryfile.read()
                            now = time.time()
                            # execute query
                            cursor.execute(query)
                            results = cursor.fetchall()

                            duration  = time.time() - now
                            # Append the result to file
                            f = open (r,'a')
                            resultline = tname+","+i+","+name + "," +  run.__str__() + "," + duration.__str__()+"\n"
                            f.write(resultline)
                            f.close()
        cursor.close()

    except Exception, e:
        logging.error ("Could not execute: " + e.__str__())

if __name__ == "__main__":
    if len(sys.argv) < 2:
        logging.fatal("Usage: <%s> <config file>" , sys.argv[0])
        sys.exit(-1)

    Config = ConfigParser.ConfigParser()
    Config.read(sys.argv[1])
    host =  Config.get("connectioninfo","host")
    port = Config.get("connectioninfo","port")
    suitList = Config.get("testinfo","suits").split(",")
    iterations = Config.getint("testinfo","iterations")
    results = Config.get("testinfo", "resultsfile")
    concurrency = Config.getint("testinfo","concurrency")

    threads = []
    while concurrency > 0:
        name = "Thread #" + concurrency.__str__()
        try:
            thread = Thread (target=suiterun,args = (name,host,port,suitList,iterations,results))
            threads.append(thread)
            #thread.start_new_thread(suiterun,(name,host,port,suitList,iterations,results,))
        except:
            logging.fatal("Unable to start thread %s", name)
        concurrency = concurrency-1
    for t in threads:
        t.start()
    for t in threads:
        t.join()



