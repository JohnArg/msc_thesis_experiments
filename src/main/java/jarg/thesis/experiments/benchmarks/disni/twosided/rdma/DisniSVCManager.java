package jarg.thesis.experiments.benchmarks.disni.twosided.rdma;

import com.ibm.disni.RdmaEndpoint;
import com.ibm.disni.verbs.*;
import jarg.jrcm.networking.dependencies.netbuffers.NetworkBufferManager;
import jarg.jrcm.networking.dependencies.netrequests.WorkRequestProxy;
import jarg.jrcm.networking.dependencies.netrequests.impl.postrecv.TwoSidedRecvRequest;
import jarg.jrcm.networking.dependencies.netrequests.impl.postsend.TwoSidedSendRequest;
import jarg.jrcm.networking.dependencies.netrequests.types.WorkRequestType;
import jarg.jrcm.networking.dependencies.svc.AbstractSVCManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static jarg.jrcm.networking.dependencies.netrequests.types.WorkRequestType.TWO_SIDED_RECV;
import static jarg.jrcm.networking.dependencies.netrequests.types.WorkRequestType.TWO_SIDED_SEND_SIGNALED;

public class DisniSVCManager extends AbstractSVCManager {
    private static final Logger logger = LoggerFactory.getLogger(DisniSVCManager.class.getSimpleName());

    public SVCPostSend[] twoSidedSendSVCs;                     // A two-sided send SVC for each WR id
    public SVCPostRecv[] twoSidedRecvSVCs;                     // A two-sided recv SVC for each WR id
    public long currentTimeStamp;


    public DisniSVCManager(int maxBufferSize, int maxWorkRequests){
        super(maxBufferSize, maxWorkRequests);
        currentTimeStamp = 0;
    }

    @Override
    public void initializeSVCs() {
        // get dependencies
        int maxWorkRequests = getMaxWorkRequests();
        int maxBufferSize = getMaxBufferSize();
        IbvMr registeredMemoryRegion = getRegisteredMemoryRegion();
        NetworkBufferManager bufferManager = getBufferManager();
        RdmaEndpoint rdmaEndpoint = getRdmaEndpoint();

        TwoSidedSendRequest twoSidedSendRequest;
        TwoSidedRecvRequest twoSidedRecvRequest;
        twoSidedSendSVCs = new SVCPostSend[maxWorkRequests];
        twoSidedRecvSVCs = new SVCPostRecv[maxWorkRequests];
        List<IbvSendWR> sendRequests;
        List<IbvRecvWR> recvRequests;

        for(int i=0; i < maxWorkRequests; i++){
            // We need to store an SVC for one request at a time, so
            // we need new lists each time
            sendRequests = new ArrayList<>(maxWorkRequests);
            recvRequests = new ArrayList<>(maxWorkRequests);

            twoSidedSendRequest = new TwoSidedSendRequest(registeredMemoryRegion);
            twoSidedSendRequest.prepareRequest();
            twoSidedSendRequest.setRequestId(i);
            twoSidedSendRequest.setSgeLength(maxBufferSize);
            twoSidedSendRequest.setBufferMemoryAddress(
                    bufferManager.getWorkRequestBufferAddress(TWO_SIDED_SEND_SIGNALED, i));
            sendRequests.add(twoSidedSendRequest.getSendWR());

            twoSidedRecvRequest = new TwoSidedRecvRequest(registeredMemoryRegion);
            twoSidedRecvRequest.prepareRequest();
            twoSidedRecvRequest.setRequestId(i);
            twoSidedRecvRequest.setSgeLength(maxBufferSize);
            twoSidedRecvRequest.setBufferMemoryAddress(
                    bufferManager.getWorkRequestBufferAddress(TWO_SIDED_RECV, i));
            recvRequests.add(twoSidedRecvRequest.getRecvWR());
            // create and store SVCs
            try {
                twoSidedSendSVCs[i] = rdmaEndpoint.postSend(sendRequests);
                twoSidedRecvSVCs[i] = rdmaEndpoint.postRecv(recvRequests);
                // post receive operations now, before starting communications
                twoSidedRecvSVCs[i].execute();
            } catch (IOException e) {
                logger.error("Failed to initialize SVCs.", e);
            }
        }
    }

    @Override
    public boolean executeSVC(WorkRequestProxy workRequestProxy) {
        boolean success = true;
        int workRequestId = workRequestProxy.getId();
        int dataLength = workRequestProxy.getBuffer().limit();
        WorkRequestType workRequestType = workRequestProxy.getWorkRequestType();

        try {
            switch (workRequestType) {
                case TWO_SIDED_SEND_SIGNALED:
                    twoSidedSendSVCs[workRequestId].getWrMod(0).
                            getSgeMod(0).setLength(dataLength);
                    currentTimeStamp = System.nanoTime();
                    twoSidedSendSVCs[workRequestId].execute();
                    break;
                case TWO_SIDED_RECV:
                    twoSidedRecvSVCs[workRequestId].execute();
                    break;
            }
        }catch(IOException e){
            logger.error("Failed to execute SVC for Work Request Type "
                    + workRequestType + " and id " + workRequestId, e);
            success = false;
        }
        return success;
    }

    /**
     * Get the timestamp of the last SEND SVC called.
     * @return
     */
    public long getCurrentTimeStamp() {
        return currentTimeStamp;
    }
}
