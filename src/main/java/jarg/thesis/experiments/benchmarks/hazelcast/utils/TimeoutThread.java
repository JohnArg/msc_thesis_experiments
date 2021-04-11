package jarg.thesis.experiments.benchmarks.hazelcast.utils;

import com.hazelcast.core.HazelcastInstance;

/**
 * Shuts down a Hazelcast instance after a timeout
 */
public class TimeoutThread extends Thread{
    private int timeout;
    private HazelcastInstance hazelcastInstance;

    public TimeoutThread(int timeout, HazelcastInstance hazelcastInstance) {
        this.timeout = timeout;
        this.hazelcastInstance = hazelcastInstance;
    }

    public void run(){
        long startTime = System.currentTimeMillis();
        long stopTime = startTime;
        long elapsedTime = 0;

        do {
            try {
                Thread.sleep(timeout - elapsedTime);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            stopTime = System.currentTimeMillis();
            elapsedTime = stopTime - startTime;
            // Might have been interrupted before timeout
        }while(elapsedTime < timeout);
        // time to shutdown the instance
        hazelcastInstance.shutdown();
    }
}
