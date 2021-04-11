package jarg.thesis.experiments.benchmarks.utils;

import java.io.InputStream;
import java.util.Properties;

public class RdmaBenchmarkConfig {
    /* ********************************************************
     *   Configuration Parameters
     * ********************************************************/
    /**
     * Endpoint connection timeout (DiSNI property).
     */
    private int timeout;
    /**
     * Endpoint polling mode (DiSNI property).
     */
    private boolean polling;
    /**
     * Endpoint max RDMA Work Requests (DiSNI property).
     */
    private int maxWRs;
    /**
     * Endpoint Scatter/Gather elements (DiSNI property).
     */
    private int maxSge;
    /**
     * Endpoint RDMA Completion Queue size (DiSNI property).
     */
    private int cqSize;
    /**
     * The server's backlog.
     */
    private int serverBacklog;
    /**
     * Endpoint max buffer size for storing messages.
     */
    private int maxBufferSize;

    public RdmaBenchmarkConfig(){ }


    public RdmaBenchmarkConfig(int timeout, boolean polling,
                      int maxWRs, int maxSge, int cqSize, int serverBacklog, int maxBufferSize) {
        this.timeout = timeout;
        this.polling = polling;
        this.maxWRs = maxWRs;
        this.maxSge = maxSge;
        this.cqSize = cqSize;
        this.serverBacklog = serverBacklog;
        this.maxBufferSize = maxBufferSize;
    }

    /**
     * Load RDMA settings from a {@link Properties} file.
     * @param filename the name of the file to read the properties from.
     * @throws Exception
     */
    public void loadFromProperties(String filename) throws Exception {
        InputStream fileInputStr = getClass().getClassLoader().getResourceAsStream(filename);
        Properties properties = new Properties();
        properties.load(fileInputStr);

        timeout = Integer.parseInt(properties.getProperty("rdma.timeout"));
        polling = Boolean.parseBoolean(properties.getProperty("rdma.polling"));
        maxWRs = Integer.parseInt(properties.getProperty("rdma.maxWRs"));
        maxSge = Integer.parseInt(properties.getProperty("rdma.maxSge"));
        cqSize = Integer.parseInt(properties.getProperty("rdma.cqSize"));
        serverBacklog = Integer.parseInt(properties.getProperty("rdma.serverBacklog"));
        maxBufferSize = Integer.parseInt(properties.getProperty("rdma.maxBufferSize"));
    }


    public int getTimeout() {
        return timeout;
    }

    public int getMaxWRs() {
        return maxWRs;
    }

    public int getMaxSge() {
        return maxSge;
    }

    public int getCqSize() {
        return cqSize;
    }

    public int getServerBacklog() {
        return serverBacklog;
    }

    public int getMaxBufferSize() {
        return maxBufferSize;
    }


    public boolean isPolling() {
        return polling;
    }

    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }

    public void setPolling(boolean polling) {
        this.polling = polling;
    }

    public void setMaxWRs(int maxWRs) {
        this.maxWRs = maxWRs;
    }

    public void setMaxSge(int maxSge) {
        this.maxSge = maxSge;
    }

    public void setCqSize(int cqSize) {
        this.cqSize = cqSize;
    }

    public void setServerBacklog(int serverBacklog) {
        this.serverBacklog = serverBacklog;
    }

    public void setMaxBufferSize(int maxBufferSize) {
        this.maxBufferSize = maxBufferSize;
    }

    @Override
    public String toString() {
        return  "# RdmaBenchmarkConfig{" + "\r\n" +
                "# timeout=" + timeout + "\r\n" +
                "# polling=" + polling + "\r\n" +
                "# maxWRs=" + maxWRs + "\r\n" +
                "# maxSge=" + maxSge + "\r\n" +
                "# cqSize=" + cqSize + "\r\n" +
                "# serverBacklog=" + serverBacklog + "\r\n" +
                "# maxBufferSize=" + maxBufferSize + "\r\n" +
                "# }\r\n";
    }
}
