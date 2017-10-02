# testing-kafka-producer

Simple testing framework that produces msgs to kafka

## Usage

```
Usage: ./kafka-producer -zk ZK_CONNECT [-rate NUM_MSG_PER_SEC] [-duration DURATION] [-dryrun]  -debug
       	Debug mode
     -duration string
       	Time lapse to send the logs
     -help
       	prints usage
     -rate int
       	msg rate per sec (default 10)
     -zk string
       	zookeeper connect string, typically zk1:2181,zk2:2181,zk3:2181/kafka

```