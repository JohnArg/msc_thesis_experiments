package jarg.thesis.experiments.benchmarks.disni.twosided.rdma;

import com.ibm.disni.RdmaServerEndpoint;
import com.ibm.disni.verbs.IbvWC;
import jarg.jrcm.networking.dependencies.netrequests.WorkCompletionHandler;
import jarg.thesis.experiments.benchmarks.disni.twosided.server.TwoSidedServer;
import jarg.thesis.experiments.benchmarks.utils.FileExporter;
import jarg.thesis.experiments.benchmarks.utils.Latency;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

public class ServerCQNotificationHandler implements WorkCompletionHandler {
    private Logger logger = LoggerFactory.getLogger(ServerCQNotificationHandler.class.getSimpleName());
    private TwoSidedServer twoSidedServer;
    private int maxIterations;
    private int currentIteration;
    private Latency[] latencies;    // currently recorded latencies
    private long beginningHeapSize;
    private long totalHeapSize;
    private FileExporter fileExporter;
    private DisniEndpoint communicationsEndpoint;

    public ServerCQNotificationHandler(){
        this.currentIteration = 0;
        this.beginningHeapSize = Runtime.getRuntime().totalMemory();
        this.totalHeapSize = Runtime.getRuntime().maxMemory();
        this.fileExporter = new FileExporter("disni_hrtt_s.dat");
        StringBuilder fileHeadings = new StringBuilder();
        fileHeadings.append("# Server beginning heap size (bytes) : " + beginningHeapSize + "\r\n");
        fileHeadings.append("# Server max heap size (bytes) : " + totalHeapSize + "\r\n");
        fileHeadings.append("# Server processing latencies ===================\r\n");
        fileExporter.exportText(fileHeadings.toString());
    }

    @Override
    public void handleCqEvent(IbvWC wc) {
        int wcOpcode = wc.getOpcode();

        if (wcOpcode == IbvWC.IbvWcOpcode.IBV_WC_RECV.getOpcode()) {

            // record server processing time ---------------------------------
            Latency latency = latencies[currentIteration];
            latency.recordTimeStart();
            // echo the received message => copy to send buffer
            communicationsEndpoint.sendBuffer.clear();
            for(int j=0; j < communicationsEndpoint.receiveBuffer.limit(); j ++){
                communicationsEndpoint.sendBuffer.put(communicationsEndpoint.receiveBuffer.get());
            }
            // prepare send buffers
            communicationsEndpoint.sendBuffer.flip();
            // repost a recv
            try {
                communicationsEndpoint.receiveBuffer.clear();
                communicationsEndpoint.recvSVC.execute();
            } catch (IOException e) {
                logger.error("Cannot repost RECV.");
            }
            // stop recording server processing time --------------------------
            latency.recordTimeEnd();

            // send the response
            try {
                communicationsEndpoint.sendSVC.execute();
            } catch (IOException e) {
                logger.error("Error in executing SendSVC. Exiting.");
                twoSidedServer.shutdown();
            }
            currentIteration ++;
            // shutdown
            if(currentIteration == maxIterations){
                fileExporter.exportLatencies(latencies);
                twoSidedServer.shutdown();
            }
        }
    }

    @Override
    public void handleCqEventError(IbvWC wc) {
        int status = wc.getStatus();
        logger.error("Cot completion event error. Status : " + status);
    }

    public void setCommunicationsEndpoint(DisniEndpoint communicationsEndpoint) {
        this.communicationsEndpoint = communicationsEndpoint;
    }

    public void setTwoSidedServer(TwoSidedServer twoSidedServer) {
        this.twoSidedServer = twoSidedServer;
        this.maxIterations = twoSidedServer.getIterations();
        this.latencies = new Latency[maxIterations];
        for(int i=0; i<latencies.length; i++){
            latencies[i] = new Latency();
        }
    }
}
