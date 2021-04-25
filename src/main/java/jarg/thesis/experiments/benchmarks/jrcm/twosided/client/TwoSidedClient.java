package jarg.thesis.experiments.benchmarks.jrcm.twosided.client;

import com.ibm.disni.RdmaActiveEndpointGroup;
import jarg.jrcm.networking.communicators.impl.ActiveRdmaCommunicator;
import jarg.jrcm.networking.dependencies.netrequests.WorkRequestProxy;
import jarg.thesis.experiments.benchmarks.utils.FileExporter;
import jarg.thesis.experiments.benchmarks.utils.Latency;
import jarg.thesis.experiments.benchmarks.utils.RdmaBenchmarkConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

import static jarg.jrcm.networking.dependencies.netrequests.types.WorkRequestType.TWO_SIDED_SEND_SIGNALED;

/**
 * A client that uses two-sided RDMA operations to communicate with a server.
 */
public class TwoSidedClient {
    private static final Logger logger = LoggerFactory.getLogger(TwoSidedClient.class.getSimpleName());
    private final String EXPORT_FILE = "rdmalib_latencies_client.dat";

    private String serverHost;
    private String serverPort;
    private RdmaActiveEndpointGroup<ActiveRdmaCommunicator> endpointGroup;
    private ClientEndpointFactory factory;
    private ActiveRdmaCommunicator clientEndpoint;
    private RdmaBenchmarkConfig config;
    private int iterations;
    private int currentIteration;
    private Latency[] latencies;        // currently recorded latencies
    private Boolean messageMonitor;     // used to for waiting on its monitor
    private boolean messageReceived;    // whether a message was received
    private byte[] messageBytes;        // the message data to be sent
    private long beginningHeapSize;
    private long totalHeapSize;
    private FileExporter fileExporter;

    public TwoSidedClient(String host, String port, RdmaBenchmarkConfig config, int iterations){
        this.serverHost = host;
        this.serverPort = port;
        this.config = config;
        this.iterations = iterations;
        this.currentIteration = 0;
        this.latencies = new Latency[iterations];
        this.messageMonitor = true;
        this.messageReceived = false;
        this.beginningHeapSize = Runtime.getRuntime().totalMemory();
        this.totalHeapSize = Runtime.getRuntime().maxMemory();
        this.fileExporter = new FileExporter("jrcm_hrrt_c.dat");
    }

    /**
     * Initializes client.
     * @throws IOException
     */
    public void init() throws IOException {
        // Create endpoint
        endpointGroup =  new RdmaActiveEndpointGroup<>(config.getTimeout(), config.isPolling(),
                config.getMaxWRs(), config.getMaxSge(), config.getCqSize());
        factory = new ClientEndpointFactory(endpointGroup,config.getMaxBufferSize(), config.getMaxWRs(), this);
        endpointGroup.init(factory);
        clientEndpoint = endpointGroup.createEndpoint();
    }

    /**
     * Runs the client benchmark.
     * @throws Exception
     */
    public void operate() throws Exception {
        // Connect to server ------------------------------------
        InetAddress serverIp = InetAddress.getByName(serverHost);
        InetSocketAddress serverSockAddr = new InetSocketAddress(serverIp,
                Integer.parseInt(serverPort));
        clientEndpoint.connect(serverSockAddr, 1000);
        logger.info("Client connected to server in address : "
                + clientEndpoint.getDstAddr().toString());

        // Prepare ------------------------------------
        StringBuilder fileHeadings = new StringBuilder();
        fileHeadings.append("# Benchmark settings ========\r\n");
        fileHeadings.append("# Beginning heap size (bytes) : " + beginningHeapSize + "\r\n");
        fileHeadings.append("# Max heap size (bytes) : " + totalHeapSize + "\r\n");
        fileHeadings.append("# Iterations : "+ iterations + "\r\n");
        fileHeadings.append(config.toString());
        fileHeadings.append("# Results ===================\r\n");
        fileExporter.exportText(fileHeadings.toString());
        // Prepare a message to send
        messageBytes = new byte[config.getMaxBufferSize()];
        for(int i=0; i<config.getMaxBufferSize(); i++){
            messageBytes[i] = 1;
        }
        // pre-create latency objects. We will be reusing them during the benchmark.
        for(int i=0; i < latencies.length; i++){
            latencies[i] = new Latency();
        }
        // measure the full time of the benchmark as well
        Latency fullBenchmarkTime = new Latency();
        fullBenchmarkTime.recordTimeStart();

        // Start benchmark -----------------------------------------------------------------------------
        for(currentIteration = 0; currentIteration < iterations; currentIteration++){
            // record timestamp
            Latency latency = latencies[currentIteration];
            latency.recordTimeStart();
            // get free Work Request id for a 'send' operation
            WorkRequestProxy workRequestProxy = clientEndpoint.getWorkRequestProxyProvider()
                    .getPostSendRequestBlocking(TWO_SIDED_SEND_SIGNALED);
            // fill tha data buffer with data to send across
            ByteBuffer sendBuffer = workRequestProxy.getBuffer();
            // fill buffer with data
            sendBuffer.put(messageBytes);
            sendBuffer.flip();
            // send the data across
            workRequestProxy.post();
            // wait for response
            synchronized (messageMonitor){
                while(messageReceived == false) {
                    try {
                        messageMonitor.wait();
                    } catch (InterruptedException e) {
                        //ignore
                    }
                }
                // reset
                messageReceived = false;
            }
        }
        // Finish benchmark --------------------------------------------------------------------------
        fullBenchmarkTime.recordTimeEnd();
        logger.info("Test took : " + ((float)fullBenchmarkTime.getLatency()/1000000000) + " seconds." );
        // Export recorded latencies
        fileExporter.exportLatencies(latencies);
        // let's shut down
        shutdown();
    }

    /**
     * Uses a {@link jarg.jrcm.networking.dependencies.netrequests.WorkCompletionHandler
     * WorkCompletionHandler}, which runs on another thread, to notify this thread about a
     * message reception. It also takes care of recording a timestamp for a message reception,
     * checking echoed data for correctness, and exporting recorded latencies to a file, if
     * necessary.
     * @param workRequestProxy the proxy that represents the RDMA RECV Work Request for which
     *                         a completion event was fired.
     */
    public void notifyOnMessageReception(WorkRequestProxy workRequestProxy){
        // set the receive timestamp
        latencies[currentIteration].recordTimeEnd();
        // check correctness of data
        ByteBuffer receiveBuffer = workRequestProxy.getBuffer();
        for(int i=0; i < messageBytes.length; i++){
            if(messageBytes[i] != receiveBuffer.get()){
                logger.error("The echoed message data is incorrect. Exiting..");
                shutdown();
                System.exit(1);
            }
        }
        workRequestProxy.releaseWorkRequest();
        // if everything is correct, wake up the client to move to the next iteration
        synchronized (messageMonitor){
            messageReceived = true;
            messageMonitor.notify();
        }
    }

    /**
     * Closes communication resources.
     */
    public void shutdown(){
        //close endpoint/group
        try {
            clientEndpoint.close();
        } catch (IOException | InterruptedException e) {
           logger.error("Error in closing endpoint.", e);
        }

        try {
            endpointGroup.close();
        } catch (IOException | InterruptedException e) {
            logger.warn("Error in closing endpoint group", e);
        }

        System.exit(0);
    }
}
