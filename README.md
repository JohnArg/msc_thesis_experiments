# Experiments For MSc Thesis

## Purpose

This project was created for some experiments/benchmarks
conducted for a MSc Thesis with title 
"Efficient State Machine Replication With RDMA RPCs in Java".
It has two parts :

1. Half round trip time (RTT/2) benchmarks.
2. A [Hazelcast IMDG](https://github.com/JohnArg/hazelcast) server that can 
   be run until a time out.


### RTT/2 Benchmarks

These are client server benchmarks that measure half 
round trip times. These benchmarks were made to compare RRT/2 times for :
    
* the [DiSNI library](https://github.com/zrlio/disni) (RDMA networking - SEND/RECV)
* the [jRCM library](https://github.com/JohnArg/jrcm) (RDMA networking - SEND/RECV)
* TCP Java NIO sockets

The classes used to execute the benchmarks are :

* DiSNI benchmark - <i>jarg/thesis/experiments/benchmarks/disni/twosided/TwoSidedBenchmark.java</i>
* jRCM benchmark - <i>jarg/thesis/experiments/benchmarks/jrcm/twosided/TwoSidedBenchmark.java</i>
* TCP NIO benchmark - <i>jarg/thesis/experiments/benchmarks/javanio/NioBenchmark.java</i>

These classes contain comments on how to run the benchmarks.

For the DiSNI and jRCM benchmarks configure the <i>rdma.bench.properties</i>
file in <i>src/main/resources/</i>. The file has comments explaining the configuration
parameters.

### Hazelcast Server

You can use the 
<i>src/main/java/jarg/thesis/experiments/benchmarks/hazelcast/TimedHazelcastInstance.java</i>
class to run a Hazelcast server until a specified timeout.
To configure the server, use the <i>hazelcast.xml</i> file in <i>src/main/resources/</i>. 
