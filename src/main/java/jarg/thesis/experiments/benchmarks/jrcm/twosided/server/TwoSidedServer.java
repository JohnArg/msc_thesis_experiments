package jarg.thesis.experiments.benchmarks.jrcm.twosided.server;

import com.ibm.disni.RdmaActiveEndpointGroup;
import com.ibm.disni.RdmaServerEndpoint;
import jarg.jrcm.networking.communicators.impl.ActiveRdmaCommunicator;
import jarg.thesis.experiments.benchmarks.utils.RdmaBenchmarkConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

/**
 * A server that uses two-sided RDMA communications to talk with clients.
 */
public class TwoSidedServer {
    private static final Logger logger = LoggerFactory.getLogger(TwoSidedServer.class.getSimpleName());

    private String serverHost;
    private String serverPort;
    private RdmaActiveEndpointGroup<ActiveRdmaCommunicator> endpointGroup;
    private ServerEndpointFactory factory;
    private RdmaServerEndpoint<ActiveRdmaCommunicator> serverEndpoint;
    private List<ActiveRdmaCommunicator> clients;
    private RdmaBenchmarkConfig config;
    private int iterations;

    public TwoSidedServer(String host, String port, RdmaBenchmarkConfig config, int iterations){
        this.serverHost = host;
        this.serverPort = port;
        this.config = config;
        this.iterations = iterations;
    }

    /**
     * Initializes the server.
     * @throws Exception
     */
    public void init() throws Exception {

        clients = new ArrayList<>();
        // Create endpoint
        endpointGroup = new RdmaActiveEndpointGroup<>(config.getTimeout(), config.isPolling(),
                config.getMaxWRs(), config.getMaxSge(), config.getCqSize());
        factory = new ServerEndpointFactory(endpointGroup, config.getMaxBufferSize(), config.getMaxWRs(), iterations,
                this);
        endpointGroup.init(factory);
        serverEndpoint = endpointGroup.createServerEndpoint();

        // bind server to address/port
        InetAddress serverIp = InetAddress.getByName(serverHost);
        InetSocketAddress serverSockAddr = new InetSocketAddress(serverIp,
                Integer.parseInt(serverPort));
        serverEndpoint.bind(serverSockAddr, config.getServerBacklog());
        logger.info("Server bound to address : "
                + serverSockAddr.toString());
    }

    /**
     * Runs the server operation.
     * @throws Exception
     */
    public void operate() throws Exception {

        while(true){
            // accept client connection
            ActiveRdmaCommunicator clientEndpoint = serverEndpoint.accept();
            clients.add(clientEndpoint);
            logger.info("Client connection accepted. Client : "
                    + clientEndpoint.getDstAddr().toString());
        }
    }

    /**
     * Closes communication resources.
     */
    public void shutdown(){
        try {
            for (ActiveRdmaCommunicator clientEndpoint : clients) {
                clientEndpoint.close();
            }
            serverEndpoint.close();
            logger.info("Server is shut down");
        }catch (Exception e){
            logger.error("Error in closing server endpoint.", e);
        }
        System.exit(0);
    }

    public int getIterations() {
        return iterations;
    }

    public RdmaBenchmarkConfig getConfig() {
        return config;
    }
}
