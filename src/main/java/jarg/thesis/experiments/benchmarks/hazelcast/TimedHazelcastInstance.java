package jarg.thesis.experiments.benchmarks.hazelcast;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.networking.rdma.RdmaConfig;
import jarg.thesis.experiments.benchmarks.hazelcast.utils.RdmaConfigSupplier;
import jarg.thesis.experiments.benchmarks.hazelcast.utils.TimeoutThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs a Hazelcast server for a specified period of time.
 */
public class TimedHazelcastInstance {
    private static final Logger logger = LoggerFactory.getLogger(TimedHazelcastInstance.class.getSimpleName());

    public static void main(String[] args) {
        if(args.length < 4){
            logger.info("Usage : <rdma IP> <discovery service IP> <discovery service port> <timeout>");
            System.exit(1);
        }

        // turn seconds in milliseconds
        int timeout = Integer.parseInt(args[3]) * 1000;

        RdmaConfigSupplier rdmaConfigSupplier = new RdmaConfigSupplier(args);
        RdmaConfig rdmaConfig = rdmaConfigSupplier.get();

        if(rdmaConfig == null){
            logger.error("Cannot create RDMA config. Shutting down.");
            System.exit(1);
        }

        logger.info("Starting Hazelcast..");
        HazelcastInstance instance = Hazelcast.newHazelcastInstanceWithRdmaConfig(rdmaConfig);

        TimeoutThread timeoutThread = new TimeoutThread(timeout, instance);
        timeoutThread.start();
        try {
            timeoutThread.join();
        } catch (InterruptedException e) {
            // ignore
        }
        System.exit(0);
    }
}
