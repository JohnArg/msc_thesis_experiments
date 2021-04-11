package jarg.thesis.experiments.benchmarks.disni.twosided.rdma;

import com.ibm.disni.RdmaActiveEndpoint;
import com.ibm.disni.RdmaActiveEndpointGroup;
import com.ibm.disni.verbs.IbvWC;
import com.ibm.disni.verbs.RdmaCmId;
import jarg.thesis.experiments.benchmarks.disni.twosided.server.TwoSidedServer;
import jarg.thesis.experiments.benchmarks.utils.FileExporter;
import jarg.thesis.experiments.benchmarks.utils.Latency;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class DisniServerEndpoint extends BasicDisniEndpoint{
    private static final Logger logger = LoggerFactory.getLogger(DisniServerEndpoint.class.getSimpleName());
    private int maxIterations;
    private int currentIteration;
    private TwoSidedServer server;
    private Latency[] latencies;    // currently recorded latencies
    private long beginningHeapSize;
    private long totalHeapSize;
    private FileExporter fileExporter;

    public DisniServerEndpoint(RdmaActiveEndpointGroup<? extends RdmaActiveEndpoint> group, RdmaCmId idPriv,
                               boolean serverSide, int maxBufferSize, TwoSidedServer server) throws IOException {
        super(group, idPriv, serverSide);
        this.maxIterations = server.getIterations();
        this.currentIteration = 0;
        this.server = server;
        this.beginningHeapSize = Runtime.getRuntime().totalMemory();
        this.totalHeapSize = Runtime.getRuntime().maxMemory();
        this.fileExporter = new FileExporter("disni_hrtt_s.dat");
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
    public void dispatchCqEvent(IbvWC wc) throws IOException {
        int status = wc.getStatus();
        int wcOpcode = wc.getOpcode();

        if(status != 0){    // an error occurred
            logger.error("Cot completion event error. Status : " + status);
        }else{
            if (wcOpcode == IbvWC.IbvWcOpcode.IBV_WC_RECV.getOpcode()) {
                // record server processing time ---------------------------------
                Latency latency = latencies[currentIteration];
                latency.recordTimeStart();
                // echo the received message => copy to send buffer
                for(int j=0; j < receiveBuffer.limit(); j ++){
                    sendBuffer.put(receiveBuffer.get());
                }
                // prepare send buffers
                sendBuffer.flip();
                // stop recording server processing time --------------------------
                latency.recordTimeEnd();
                // send the response
                sendSVC.execute();
                currentIteration ++;
                // shutdown
                if(currentIteration == maxIterations){
                    fileExporter.exportLatencies(latencies);
                    server.shutdown();
                }
            }else{
                sendBuffer.clear();
            }
        }
    }
}
