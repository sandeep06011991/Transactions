
server:
	ant
	echo yes|./bin/gpServer.sh  -DgigapaxosConfig=src/testing/gigapaxos.properties  clear all
	./bin/gpServer.sh  -DgigapaxosConfig=src/testing/gigapaxos.properties  stop all
	rm -rf tmp/*
	rm -rf paxos_logs/*
	rm -rf reconfiguration_DB/*
	./bin/gpServer.sh -DgigapaxosConfig=src/testing/gigapaxos.properties start all


serverd:
	ant
	./bin/gpServer.sh -DgigapaxosConfig=src/testing/gigapaxos.properties stop all
	rm -rf tmp/*
	rm -rf paxos_logs/*
	rm -rf reconfiguration_DB/*
	./bin/gpServer.sh -DgigapaxosConfig=src/testing/gigapaxos.properties -debug start all


client:
	./bin/gpClient.sh -DgigapaxosConfig=src/testing/gigapaxos.properties testing.BasicTest

sim:
	ant 
	rm -rf results
	./bin/gpClient.sh -DgigapaxosConfig=src/testing/gigapaxos.properties testing.Simulator

latency:
	./bin/gpClient.sh -DgigapaxosConfig=src/testing/gigapaxos.properties testing.Latency


kill1:
	ps -e| grep java | awk 'BEGIN {}{print $$1}' | xargs  kill -9


kill_all:
	./bin/gpServer.sh  -DgigapaxosConfig=src/testing/gigapaxos.properties  stop all

restart:
	./bin/gpServer.sh -DgigapaxosConfig=src/testing/gigapaxos.properties start all

clear:
	./bin/gpServer.sh -DgigapaxosConfig=src/testing/gigapaxos.properties clear all


getLogs:
	rm -rf tmp/server1/
	mkdir tmp/server1
	scp -r oversky@128.110.154.90:~/CalculatorTX/tmp/ tmp/server1
	rm -rf tmp/server2/
	mkdir tmp/server2
	scp -r oversky@128.110.154.116:~/CalculatorTX/tmp/ tmp/server2
	rm -rf tmp/server3/
	mkdir tmp/server3
	scp -r oversky@128.110.154.103:~/CalculatorTX/tmp/ tmp/server3

clear_everything:
	ssh oversky@128.110.154.147 "rm -rf CalculatorTX/"
	ssh oversky@128.110.154.224 "rm -rf CalculatorTX/"
	ssh oversky@128.110.154.161 "rm -rf CalculatorTX/"
	ssh oversky@128.110.154.237 "rm -rf CalculatorTX/"
	ssh oversky@128.110.154.137 "rm -rf CalculatorTX/"
	ssh oversky@128.110.154.239 "rm -rf CalculatorTX/"
	ssh oversky@128.110.154.212 "rm -rf CalculatorTX/"
	ssh oversky@128.110.154.153 "rm -rf CalculatorTX/"
	ssh oversky@128.110.154.211 "rm -rf CalculatorTX/"
	ssh oversky@128.110.154.127 "rm -rf CalculatorTX/"
