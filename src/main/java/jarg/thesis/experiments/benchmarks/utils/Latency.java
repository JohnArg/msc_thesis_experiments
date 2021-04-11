package jarg.thesis.experiments.benchmarks.utils;

public class Latency {

    private long timeStart; // nano seconds
    private long timeEnd;   // nano seconds
    private long latency;   // nano seconds

    public Latency() {
    }

    public void recordTimeStart() {
        this.timeStart = System.nanoTime();
    }

    public void recordTimeEnd() {
        this.timeEnd = System.nanoTime();
    }

    public long getLatency() {
        this.latency = timeEnd - timeStart;
        return latency;
    }
}
