#!/usr/bin/python

import random,os,time,datetime

def start_server(server_name):
    os.system('./bin/gpServer.sh -DgigapaxosConfig=./conf/gigapaxos.properties start '+server_name + " &")

def stop_server(server_name):
    os.system('./bin/gpServer.sh -DgigapaxosConfig=./conf/gigapaxos.properties stop '+server_name + " &")

'''
    Test 1
'''
MTTR = 60

def get_time():
    ts = time.time()
    return  datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')

def loop(E_mttf,server_name):
    while(True):
        print "Starting server {} at {}".format(server_name,get_time())
        start_server(server_name)
        wait = random.expovariate(1.0/E_mttf)
        time.sleep(wait)
        print "Stopping server {} at {}".format(server_name,get_time())
        stop_server(server_name)
        time.sleep(MTTR)



if __name__ == "__main__":
    import sys
    E_mttf = sys.argv[1]
    server_name = sys.argv[2]
    start_server(server_name)
