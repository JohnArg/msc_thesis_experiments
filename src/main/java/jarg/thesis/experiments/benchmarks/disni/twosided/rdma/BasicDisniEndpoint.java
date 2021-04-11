package jarg.thesis.experiments.benchmarks.disni.twosided.rdma;

import com.ibm.disni.RdmaActiveEndpoint;
import com.ibm.disni.RdmaActiveEndpointGroup;
import com.ibm.disni.verbs.*;
import jarg.jrcm.networking.dependencies.netrequests.impl.postrecv.TwoSidedRecvRequest;
import jarg.jrcm.networking.dependencies.netrequests.impl.postsend.TwoSidedSendRequest;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public abstract class BasicDisniEndpoint extends RdmaActiveEndpoint {
    // buffers -----------------------------
    private IbvMr registeredMemoryRegion;
    public ByteBuffer sendBuffer;
    private long sendBufferAddress;
    public ByteBuffer receiveBuffer;
    private long receiveBufferAddress;
    private ByteBuffer registeredMemoryBuffer;
    private int maxBufferSize;
    // SVCs -----------------------------------
    public SVCPostSend sendSVC;                     // A two-sided SEND SVC
    public SVCPostRecv recvSVC;                     // A two-sided RECV SVC


    public BasicDisniEndpoint(RdmaActiveEndpointGroup<? extends RdmaActiveEndpoint> group, RdmaCmId idPriv,
                              boolean serverSide) throws IOException {
        super(group, idPriv, serverSide);
    }

    @Override
    public void init() throws IOException{
        registerCommunicationsMemory();
        registeredMemoryRegion = registerMemory(registeredMemoryBuffer).execute().free().getMr();
        createSVCs();
    }

    private void registerCommunicationsMemory(){
        int bufferArrayBytes = maxBufferSize * 2;
        registeredMemoryBuffer = ByteBuffer.allocateDirect(bufferArrayBytes);
        // register send buffer -------------------------
        int currentLimit = maxBufferSize;
        registeredMemoryBuffer.limit(currentLimit);
        sendBuffer = registeredMemoryBuffer.slice();
        registeredMemoryBuffer.position(currentLimit);
        currentLimit += maxBufferSize;
        sendBufferAddress = ((sun.nio.ch.DirectBuffer) sendBuffer).address();
        // register receive buffer ----------------------
        registeredMemoryBuffer.limit(currentLimit);
        receiveBuffer = registeredMemoryBuffer.slice();
        registeredMemoryBuffer.position(currentLimit);
        currentLimit += maxBufferSize;
        receiveBufferAddress = ((sun.nio.ch.DirectBuffer) receiveBuffer).address();
    }

    private void createSVCs(){
        TwoSidedSendRequest twoSidedSendRequest;
        TwoSidedRecvRequest twoSidedRecvRequest;
        List<IbvSendWR> sendRequests = new ArrayList<>(1);
        List<IbvRecvWR> recvRequests = new ArrayList<>(1);

        // create SEND SVC
        twoSidedSendRequest = new TwoSidedSendRequest(registeredMemoryRegion);
        twoSidedSendRequest.prepareRequest();
        twoSidedSendRequest.setRequestId(1);
        twoSidedSendRequest.setSgeLength(maxBufferSize);
        twoSidedSendRequest.setBufferMemoryAddress(sendBufferAddress);
        sendRequests.add(twoSidedSendRequest.getSendWR());
        // create RECV SVC
        twoSidedRecvRequest = new TwoSidedRecvRequest(registeredMemoryRegion);
        twoSidedRecvRequest.prepareRequest();
        twoSidedRecvRequest.setRequestId(1);
        twoSidedRecvRequest.setSgeLength(maxBufferSize);
        twoSidedRecvRequest.setBufferMemoryAddress(receiveBufferAddress);
        recvRequests.add(twoSidedRecvRequest.getRecvWR());

        try {
            sendSVC = postSend(sendRequests);
            recvSVC = postRecv(recvRequests);
            // port a recv
            recvSVC.execute();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
