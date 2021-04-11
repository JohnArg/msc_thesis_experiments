package jarg.thesis.experiments.benchmarks.hazelcast.utils;

import com.hazelcast.internal.networking.rdma.RdmaConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Supplier;

public class RdmaConfigSupplier implements Supplier<RdmaConfig> {
    private static final Logger logger = LoggerFactory.getLogger(RdmaConfigSupplier.class.getSimpleName());

    private String[] cmdArgs;

    public RdmaConfigSupplier(String[] cmdArgs) {
        this.cmdArgs = cmdArgs;
    }

    @Override
    public RdmaConfig get() {
        RdmaConfig rdmaConfig = new RdmaConfig();
        try {
            rdmaConfig.loadFromProperties("rdma.properties");
        } catch (Exception e) {
            logger.error("Cannot read RDMA properties.", e);
            return null;
        }

        // Change RDMA properties according to cmd arguments
        rdmaConfig.setRdmaAddress(cmdArgs[0]);
        rdmaConfig.setDiscoveryAddress(cmdArgs[1]);
        rdmaConfig.setDiscoveryPort(Integer.parseInt(cmdArgs[2]));

        return rdmaConfig;
    }
}
