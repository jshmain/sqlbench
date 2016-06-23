#!/usr/bin/python

from impala.dbapi import connect
import logging
import sys
import ConfigParser
from threading import Thread
import time
import os
from random import randint


logging.basicConfig(stream=sys.stdout, level=logging.INFO)

LOG = logging.getLogger(__name__)

def suiterun (tname,hosts,p,suits,run,r):
    # Open Impala Connection
    logging.info ("Running %s", tname)

    # chose impala daemon randomly from the list
   # subscript = randint(0,len(h)-1)
   # h = h[subscript]
    conns = []
    try:
        for h in hosts:
            conn = connect (host=h,port=p)
            conns.append(conn)
        # Run dictates how many times to run all the suits
        # runs are executed serially
        while run > 0:
            run = run  -1
            # For each run we iterate through configured Suits and execute them
            for i in suits:
                # suiteparams:  database:suite_name
                suitparams = i.split (":")
                topdir = "/appl/perfbench/latest/suites/" + suitparams[1] + "/impala"
                # For every suit there is a folder that gets processed
                # In that folder we will find a bunch of files with queries
                for root,dirs,files  in os.walk(topdir,topdown="False"):
                    for name in files:
                        with open(os.path.join(root, name),'r') as queryfile:
                            query = queryfile.read()
                            #chose impala daemon randomly from the list
                            #this is done for load balancing purpose
                            subscript = randint(0,len(conns)-1)
                            ############ get cursor
                            cursor = conns[subscript].cursor()
                            now = time.time()
                            # execute query
                            cursor.execute("use " + suitparams[0])
                            cursor.execute(query)
                            # we fetch all of the results to simulate a real client
                            results = cursor.fetchall()
                            cursor.close()
                            ############ close cursor
                            duration  = time.time() - now
                            # Append the result to file
                            f = open (r,'a')
                            resultline = "impala,"+tname+","+suitparams[1]+","+name + "," +  run.__str__() + "," + duration.__str__()+"\n"
                            f.write(resultline)
                            f.close()

    except Exception, e:
        logging.error ("Could not execute: " + e.__str__())

if __name__ == "__main__":
    if len(sys.argv) < 2:
        logging.fatal("Usage: <%s> <config file>" , sys.argv[0])
        sys.exit(-1)

    Config = ConfigParser.ConfigParser()
    Config.read(sys.argv[1])
    host =  Config.get("connectioninfo","host").split(",")
    port = Config.get("connectioninfo","port")
    suitList = Config.get("testinfo","suits").split(",")
    iterations = Config.getint("testinfo","iterations")
    results = Config.get("testinfo", "resultsfile")
    concurrency = Config.getint("testinfo","concurrency")

    threads = []
    # Concurrency dictates # of concurrent connections to Impala
    # We will spin up a different thread per connection
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



