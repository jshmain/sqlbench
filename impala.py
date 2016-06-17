#!/usr/bin/python

from impala.dbapi import connect
import logging
import sys
import ConfigParser

logging.basicConfig(stream=sys.stdout, level=logging.INFO)

LOG = logging.getLogger(__name__)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        logging.fatal("Usage: <%s> <config file>" , sys.argv[0])
        sys.exit(-1)

    Config = ConfigParser.ConfigParser()
    Config.read(sys.argv[1])
    host =  Config.get("connection","host")
    port = Config.get("connection","port")
    suitList = Config.get("suite","list").split(",")
    iterations = Config.getint("suite","iterations")
    results = Config.get("results", "file")

    Config.get("connection","host")
    # Open Impala Connection
    conn = connect(host=host, port=port)
    cursor = conn.cursor()

    while iterations > 0:
        iterations = iterations -1
        for i in suitList:
            print ("TEST")
            # Open a suit directory
                # Open each file
                # read in query
                # Get Init Time
                # execute query
                # Result = time now - init time
                # Append the result to file

    cursor.execute('select * from customers')
    results = cursor.fetchall()
    print (results)


