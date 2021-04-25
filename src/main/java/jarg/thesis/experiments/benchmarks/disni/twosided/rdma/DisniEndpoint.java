package jarg.thesis.experiments.benchmarks.disni.twosided.rdma;

import com.ibm.disni.RdmaActiveEndpoint;
import com.ibm.disni.RdmaActiveEndpointGroup;
import com.ibm.disni.verbs.*;
import jarg.jrcm.networking.dependencies.netrequests.WorkCompletionHandler;
import jarg.jrcm.networking.dependencies.netrequests.impl.postrecv.TwoSidedRecvRequest;
import jarg.jrcm.networking.dependencies.netrequests.impl.postsend.TwoSidedSendRequest;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class DisniEndpoint extends RdmaActiveEndpoint {
    // buffers -----------------------------
    private IbvMr registeredMemoryRegion;
    public ByteBuffer sendBuffer;
    private long sendBufferAddress;
    public ByteBuffer[] receiveBuffers;
    private long[] receiveBufferAddresses;
    private ByteBuffer registeredMemoryBuffer;
    private int maxWRs;
    private int maxBufferSize;
    private WorkCompletionHandler cqNotificationHandler;
    // SVCs -----------------------------------
    public SVCPostSend sendSVC;                     // A two-sided SEND SVC
    public SVCPostRecv[] recvSVCs;                     // A two-sided RECV SVC


    public DisniEndpoint(RdmaActiveEndpointGroup<? extends RdmaActiveEndpoint> group, RdmaCmId idPriv,
                         boolean serverSide, int maxWRs, int maxBufferSize,
                         WorkCompletionHandler cqNotificationHandler) throws IOException {
        super(group, idPriv, serverSide);
        this.maxBufferSize = maxBufferSize;
        this.maxWRs = maxWRs;
        this.cqNotificationHandler = cqNotificationHandler;
        this.receiveBufferAddresses = new long[maxWRs];
        this.receiveBuffers = new ByteBuffer[maxWRs];
        this.recvSVCs = new SVCPostRecv[maxWRs];
    }

    @Override
    public void init() throws IOException{
        registerCommunicationsMemory();
        registeredMemoryRegion = registerMemory(registeredMemoryBuffer).execute().free().getMr();
        createSVCs();
    }

    private void registerCommunicationsMemory(){
        // allocate for 1 send buffer and maxWRs recv buffers
        int bufferArrayBytes = maxBufferSize * (maxWRs + 1);
        registeredMemoryBuffer = ByteBuffer.allocateDirect(bufferArrayBytes);
        // register send buffer -------------------------
        int currentLimit = maxBufferSize;
        registeredMemoryBuffer.limit(currentLimit);
        sendBuffer = registeredMemoryBuffer.slice();
        registeredMemoryBuffer.position(currentLimit);
        currentLimit += maxBufferSize;
        sendBufferAddress = ((sun.nio.ch.DirectBuffer) sendBuffer).address();
        // register receive buffers ----------------------
        for(int i=0; i<maxWRs; i++){
            registeredMemoryBuffer.limit(currentLimit);
            receiveBuffers[i] = registeredMemoryBuffer.slice();
            registeredMemoryBuffer.position(currentLimit);
            currentLimit += maxBufferSize;
            receiveBufferAddresses[i] = ((sun.nio.ch.DirectBuffer) receiveBuffers[i]).address();
        }
    }

    private void createSVCs(){
        TwoSidedSendRequest twoSidedSendRequest;
        TwoSidedRecvRequest twoSidedRecvRequest;
        List<IbvSendWR> sendRequests = new ArrayList<>(1);

        // create SEND SVC
        twoSidedSendRequest = new TwoSidedSendRequest(registeredMemoryRegion);
        twoSidedSendRequest.prepareRequest();
        twoSidedSendRequest.setRequestId(maxWRs + 1);
        twoSidedSendRequest.setSgeLength(maxBufferSize);
        twoSidedSendRequest.setBufferMemoryAddress(sendBufferAddress);
        sendRequests.add(twoSidedSendRequest.getSendWR());
        try {
            sendSVC = postSend(sendRequests);
        } catch (IOException e) {
            e.printStackTrace();
        }

        // create and pre-post RECVs
        for(int i=0; i<maxWRs; i++) {
            List<IbvRecvWR> recvRequests = new ArrayList<>(1);
            // create RECV SVC
            twoSidedRecvRequest = new TwoSidedRecvRequest(registeredMemoryRegion);
            twoSidedRecvRequest.prepareRequest();
            twoSidedRecvRequest.setRequestId(i);
            twoSidedRecvRequest.setSgeLength(maxBufferSize);
            twoSidedRecvRequest.setBufferMemoryAddress(receiveBufferAddresses[i]);
            recvRequests.add(twoSidedRecvRequest.getRecvWR());

            try {
                recvSVCs[i] = postRecv(recvRequests);
                recvSVCs[i].execute();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void dispatchCqEvent(IbvWC wc) throws IOException {
        int status = wc.getStatus();

        if(status != 0){    // an error occurred
            cqNotificationHandler.handleCqEventError(wc);
        }else{
            cqNotificationHandler.handleCqEvent(wc);
        }
    }
}
