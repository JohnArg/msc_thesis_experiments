package jarg.thesis.experiments.benchmarks.jrcm.twosided.server;

import com.ibm.disni.verbs.IbvWC;
import jarg.jrcm.networking.communicators.impl.ActiveRdmaCommunicator;
import jarg.jrcm.networking.dependencies.netrequests.AbstractWorkCompletionHandler;
import jarg.jrcm.networking.dependencies.netrequests.WorkRequestProxy;
import jarg.thesis.experiments.benchmarks.utils.FileExporter;
import jarg.thesis.experiments.benchmarks.utils.Latency;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

import static jarg.jrcm.networking.dependencies.netrequests.types.WorkRequestType.TWO_SIDED_RECV;
import static jarg.jrcm.networking.dependencies.netrequests.types.WorkRequestType.TWO_SIDED_SEND_SIGNALED;


/**
 * Handles the completion of networking requests for a server RDMA endpoint.
 */
public class ServerCompletionHandler extends AbstractWorkCompletionHandler {
    private static final Logger logger = LoggerFactory.getLogger(ServerCompletionHandler.class.getSimpleName());
    private int maxIterations;
    private int currentIteration;
    private TwoSidedServer server;
    private Latency[] latencies;    // currently recorded latencies
    private long beginningHeapSize;
    private long totalHeapSize;
    private FileExporter fileExporter;

    public ServerCompletionHandler(TwoSidedServer server) {
        super();
        this.maxIterations = server.getIterations();
        this.currentIteration = 0;
        this.server = server;
        this.beginningHeapSize = Runtime.getRuntime().totalMemory();
        this.totalHeapSize = Runtime.getRuntime().maxMemory();
        this.fileExporter = new FileExporter("jrcm_hrtt_s.dat");
        this.latencies = new Latency[maxIterations];
        for(int i=0; i<latencies.length; i++){
            latencies[i] = new Latency();
        }
        StringBuilder fileHeadings = new StringBuilder();
        fileHeadings.append("# Server beginning heap size (bytes) : " + beginningHeapSize + "\r\n");
        fileHeadings.append("# Server max heap size (bytes) : " + totalHeapSize + "\r\n");
        fileHeadings.append("# Server processing latencies ===================\r\n");
        fileExporter.exportText(fileHeadings.toString());
    }

    @Override
    public void handleCqEvent(IbvWC workCompletionEvent) {
        // associate event with a Work Request
        WorkRequestProxy receiveProxy = getProxyProvider().getWorkRequestProxyForWc(workCompletionEvent);
        // if this is a completion for a SEND
        if(receiveProxy.getWorkRequestType().equals(TWO_SIDED_SEND_SIGNALED)){
            receiveProxy.releaseWorkRequest();
            // else if this is a completion for a RECV
        }else if(receiveProxy.getWorkRequestType().equals(TWO_SIDED_RECV)){
            // record server processing time ---------------------------------
            Latency latency = latencies[currentIteration];
            latency.recordTimeStart();
            // echo the received message
            ByteBuffer receiveBuffer = receiveProxy.getBuffer();
            WorkRequestProxy responseProxy = getProxyProvider().getPostSendRequestBlocking(TWO_SIDED_SEND_SIGNALED);
            ByteBuffer sendBuffer = responseProxy.getBuffer();
            for(int j=0; j < receiveBuffer.limit(); j ++){
                sendBuffer.put(receiveBuffer.get());
            }
            // prepare send buffers
            sendBuffer.flip();
            // stop recording server processing time --------------------------
            latency.recordTimeEnd();
            // send the response
            responseProxy.post();
            // we don't need the received proxy anymore
            receiveProxy.releaseWorkRequest();
            currentIteration ++;
            // shutdown
            if(currentIteration == maxIterations){
                fileExporter.exportLatencies(latencies);
                server.shutdown();
            }
        }
    }

    @Override
    public void handleCqEventError(IbvWC workCompletionEvent) {
        // associate event with a Work Request
        WorkRequestProxy receiveProxy = getProxyProvider().getWorkRequestProxyForWc(workCompletionEvent);
        // Must free the request
        receiveProxy.releaseWorkRequest();
        // Status 5 can happen on remote side disconnect, since we have already posted
        // RECV requests for that remote side. We can simply close the remote endpoint
        // at this point.
        if(workCompletionEvent.getStatus() == IbvWC.IbvWcStatus.IBV_WC_WR_FLUSH_ERR.ordinal()){
            ActiveRdmaCommunicator communicator = (ActiveRdmaCommunicator) receiveProxy.getRdmaCommunicator();
            try {
                communicator.close();
            } catch (IOException | InterruptedException e) {
                logger.error("Error in closing endpoint.", e);
            }
        }else{
            logger.error("Error in network request "+ workCompletionEvent.getWr_id()
                    + " of status : " + workCompletionEvent.getStatus());
        }
    }

}
